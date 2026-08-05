import datetime

from unittest.mock import MagicMock

import pytest

from cloudpathlib import GSPath
from google.api_core.exceptions import NotFound

from gfw.ops.pipelines.bq_to_parquet.destination import HiveDestination
from gfw.ops.pipelines.bq_to_parquet.main import Exporter, _date_range, run
from gfw.ops.pipelines.bq_to_parquet.source import PartitionedSource, ShardedSource, Source


def _make_bq_factory(mock_client):
    def factory(project):
        return mock_client
    return factory


def _make_gcs_factory(mock_client):
    def factory(project):
        return mock_client
    return factory


def _make_gcs_client(already_exported_dates=None):
    """Return a mock GCS client. Dates in already_exported_dates simulate existing partitions."""
    already_exported_dates = already_exported_dates or set()
    mock_client = MagicMock()

    def list_blobs(bucket, prefix, delimiter):
        page = MagicMock()
        page.prefixes = [
            f"{prefix}{d.isoformat()}/" for d in already_exported_dates
        ]
        blobs = MagicMock()
        blobs.pages = [page]
        return blobs

    mock_client.list_blobs.side_effect = list_blobs
    return mock_client


def _make_bq_client(extract_job=None, dates_without_data=None):
    """Return a mock BQ client.

    extract_job:
        Value returned by extract_table(). Defaults to a MagicMock representing
        a successful job.

    dates_without_data:
        Dates that a source's dates_with_data_query() check should report as
        having no data. Defaults to none -- every date in the requested range
        is reported as having data, matching pre-existing test expectations
        that don't exercise the empty/missing-date check. Applies to whichever
        query the source under test builds (Exporter.dates_with_data() reads
        min_id/max_id from the query's parameters, shared by both source
        types), so this works for both PartitionedSource and ShardedSource.
    """
    dates_without_data = dates_without_data or set()
    mock_client = MagicMock()
    mock_client.extract_table.return_value = extract_job or MagicMock()

    def query(sql, job_config=None):
        params = {p.name: p.value for p in job_config.query_parameters}
        start = datetime.datetime.strptime(params["min_id"], "%Y%m%d").date()
        end = datetime.datetime.strptime(params["max_id"], "%Y%m%d").date()

        rows = []
        current = start
        while current <= end:
            if current not in dates_without_data:
                rows.append(MagicMock(date_id=current.strftime("%Y%m%d")))
            current += datetime.timedelta(days=1)

        mock_query_job = MagicMock()
        mock_query_job.result.return_value = rows
        return mock_query_job

    mock_client.query.side_effect = query
    return mock_client


def _make_exporter(
    mock_bq, mock_gcs, bq_in="proj.ds.table", sharded=False, event_source="wf827-table"
):
    return Exporter(
        bq_client=mock_bq,
        source=Source.create(bq_in, sharded),
        destination=HiveDestination(
            gcs_out=GSPath("gs://bucket/path"),
            event_source=event_source,
            gcs_client=mock_gcs,
        ),
    )


def test_date_range():
    dates = _date_range("2024-01-01", "2024-01-04")
    assert dates == [
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
    ]


def test_date_range_empty():
    assert _date_range("2024-01-01", "2024-01-01") == []


def test_partitioned_source_ref():
    date = datetime.date(2024, 1, 15)
    assert PartitionedSource("proj.ds.table").ref(date) == "proj.ds.table$20240115"


def test_sharded_source_ref():
    date = datetime.date(2024, 1, 15)
    assert ShardedSource("proj.ds.table").ref(date) == "proj.ds.table_20240115"


def test_source_dates_with_data_query_is_not_implemented_by_default():
    dates = [datetime.date(2024, 1, 1)]
    with pytest.raises(NotImplementedError):
        Source("proj.ds.table").dates_with_data_query(dates)


def test_partitioned_source_dates_with_data_query():
    dates = [datetime.date(2024, 1, 1), datetime.date(2024, 1, 3)]

    query = PartitionedSource("proj.ds.table").dates_with_data_query(dates)

    assert "INFORMATION_SCHEMA.PARTITIONS" in query.sql
    assert "total_rows > 0" in query.sql
    params = {p.name: p.value for p in query.parameters}
    assert params == {
        "table_name": "table",
        "min_id": "20240101",
        "max_id": "20240103",
    }


def test_partitioned_source_dates_with_data_query_empty_input():
    """Exporter never calls this with an empty list -- it short-circuits first.
    A direct call violating that precondition should fail loudly, not return
    an ambiguous None (which means something else: "this source can't check").
    """
    with pytest.raises(ValueError):
        PartitionedSource("proj.ds.table").dates_with_data_query([])


def test_sharded_source_dates_with_data_query():
    dates = [datetime.date(2024, 1, 1), datetime.date(2024, 1, 3)]

    query = ShardedSource("proj.ds.table").dates_with_data_query(dates)

    assert "INFORMATION_SCHEMA.TABLES" in query.sql
    params = {p.name: p.value for p in query.parameters}
    assert params == {
        "table_prefix": "table_",
        "min_id": "20240101",
        "max_id": "20240103",
    }


def test_sharded_source_dates_with_data_query_empty_input():
    with pytest.raises(ValueError):
        ShardedSource("proj.ds.table").dates_with_data_query([])


def test_exporter_dates_with_data_empty_input():
    """remaining_dates() never calls this with an empty list -- it short-circuits
    first. A direct call violating that precondition fails loudly instead of
    returning an empty set silently.
    """
    mock_bq = MagicMock()
    exporter = _make_exporter(mock_bq, _make_gcs_client())

    with pytest.raises(ValueError):
        exporter.dates_with_data([])
    mock_bq.query.assert_not_called()


def test_run_skips_bq_check_when_all_dates_already_exported():
    mock_bq = MagicMock()
    already_exported = {datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)}

    results = _make_exporter(mock_bq, _make_gcs_client(already_exported)).run([
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
    ])

    assert results.succeeded == []
    mock_bq.query.assert_not_called()
    mock_bq.extract_table.assert_not_called()


def test_exporter_dates_with_data_filters_using_source_query():
    mock_bq = _make_bq_client(dates_without_data={datetime.date(2024, 1, 2)})
    exporter = _make_exporter(mock_bq, _make_gcs_client())
    dates = [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]

    assert exporter.dates_with_data(dates) == {
        datetime.date(2024, 1, 1), datetime.date(2024, 1, 3)
    }


def test_dry_run_does_not_submit_jobs():
    mock_bq = _make_bq_client()
    run(
        bq_in="proj.ds.table",
        gcs_out="gs://bucket/path",
        project="proj",
        event_source="wf827-table",
        start_date="2024-01-01",
        end_date="2024-01-03",
        dry_run=True,
        bq_client_factory=_make_bq_factory(mock_bq),
        gcs_client_factory=_make_gcs_factory(_make_gcs_client()),
    )
    mock_bq.extract_table.assert_not_called()


def test_run_submits_one_job_per_day():
    mock_job = MagicMock()
    mock_bq = _make_bq_client(extract_job=mock_job)

    results = _make_exporter(mock_bq, _make_gcs_client()).run([
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
    ])

    assert mock_bq.extract_table.call_count == 2
    assert len(results.succeeded) == 2
    assert mock_job.result.call_count == 2


def test_run_hive_path_includes_source_and_date():
    mock_job = MagicMock()
    mock_bq = _make_bq_client(extract_job=mock_job)

    run(
        bq_in="proj.ds.table",
        gcs_out="gs://bucket/path",
        project="proj",
        event_source="wf827-table",
        start_date="2024-01-01",
        end_date="2024-01-02",
        partition_prefix="event_",
        bq_client_factory=_make_bq_factory(mock_bq),
        gcs_client_factory=_make_gcs_factory(_make_gcs_client()),
    )

    dest = mock_bq.extract_table.call_args[0][1]
    assert dest == "gs://bucket/path/event_source=wf827-table/event_date=2024-01-01/*.parquet"


def test_run_skips_already_exported_dates():
    mock_job = MagicMock()
    mock_bq = _make_bq_client(extract_job=mock_job)
    already_exported = {datetime.date(2024, 1, 1)}

    results = _make_exporter(mock_bq, _make_gcs_client(already_exported)).run([
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
    ])

    assert mock_bq.extract_table.call_count == 1
    assert len(results.succeeded) == 1


def test_run_skips_dates_with_no_data_in_source():
    mock_job = MagicMock()
    mock_bq = _make_bq_client(
        extract_job=mock_job, dates_without_data={datetime.date(2024, 1, 1)}
    )

    results = _make_exporter(mock_bq, _make_gcs_client()).run([
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
    ])

    assert mock_bq.extract_table.call_count == 1
    assert mock_bq.extract_table.call_args[0][0] == "proj.ds.table$20240102"
    assert len(results.succeeded) == 1


def test_run_sharded_skips_dates_with_missing_shard():
    """A missing shard is caught by the proactive check, before any job is submitted."""
    mock_job = MagicMock()
    mock_bq = _make_bq_client(
        extract_job=mock_job, dates_without_data={datetime.date(2024, 1, 1)}
    )

    results = _make_exporter(mock_bq, _make_gcs_client(), sharded=True).run([
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
    ])

    assert mock_bq.extract_table.call_count == 1
    assert mock_bq.extract_table.call_args[0][0] == "proj.ds.table_20240102"
    assert len(results.succeeded) == 1


def test_run_raises_on_job_failure():
    mock_job = MagicMock()
    mock_job.result.side_effect = Exception("BQ backend error")
    mock_bq = _make_bq_client(extract_job=mock_job)

    with pytest.raises(RuntimeError, match="Export failed for 1 date"):
        run(
            bq_in="proj.ds.table",
            gcs_out="gs://bucket/path",
            project="proj",
            event_source="wf827-table",
            start_date="2024-01-01",
            end_date="2024-01-02",
            bq_client_factory=_make_bq_factory(mock_bq),
            gcs_client_factory=_make_gcs_factory(_make_gcs_client()),
        )


def test_run_raises_on_unexpected_not_found():
    """Even though missing shards are normally caught up front, a NotFound that
    slips through (e.g. a genuine race: the shard is dropped between the check
    and the job running) is treated as a hard failure, not silently skipped --
    it's no longer the expected way to detect a missing date.
    """
    mock_job = MagicMock()
    mock_job.result.side_effect = [None, NotFound("not found")]
    mock_bq = _make_bq_client(extract_job=mock_job)

    with pytest.raises(RuntimeError, match="Export failed for 1 date"):
        run(
            bq_in="proj.ds.table",
            gcs_out="gs://bucket/path",
            project="proj",
            event_source="wf827-table",
            start_date="2024-01-01",
            end_date="2024-01-03",
            sharded=True,
            bq_client_factory=_make_bq_factory(mock_bq),
            gcs_client_factory=_make_gcs_factory(_make_gcs_client()),
        )

    assert mock_bq.extract_table.call_count == 2
