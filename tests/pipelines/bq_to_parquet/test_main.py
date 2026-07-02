import datetime
import pytest
from unittest.mock import MagicMock

from google.api_core.exceptions import NotFound
from cloudpathlib import GSPath

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


def test_dry_run_does_not_submit_jobs():
    mock_bq = MagicMock()
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
    mock_bq = MagicMock()
    mock_bq.extract_table.return_value = mock_job

    results = _make_exporter(mock_bq, _make_gcs_client()).run([
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
    ])

    assert mock_bq.extract_table.call_count == 2
    assert len(results.succeeded) == 2
    assert mock_job.result.call_count == 2


def test_run_hive_path_includes_source_and_date():
    mock_job = MagicMock()
    mock_bq = MagicMock()
    mock_bq.extract_table.return_value = mock_job

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
    mock_bq = MagicMock()
    mock_bq.extract_table.return_value = mock_job
    already_exported = {datetime.date(2024, 1, 1)}

    results = _make_exporter(mock_bq, _make_gcs_client(already_exported)).run([
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
    ])

    assert mock_bq.extract_table.call_count == 1
    assert len(results.succeeded) == 1


def test_run_raises_on_job_failure():
    mock_job = MagicMock()
    mock_job.result.side_effect = Exception("BQ backend error")
    mock_bq = MagicMock()
    mock_bq.extract_table.return_value = mock_job

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


def test_run_sharded_skips_missing():
    mock_job = MagicMock()
    mock_job.result.side_effect = [None, NotFound("not found")]
    mock_bq = MagicMock()
    mock_bq.extract_table.return_value = mock_job

    results = _make_exporter(mock_bq, _make_gcs_client(), sharded=True).run([
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
    ])

    assert mock_bq.extract_table.call_count == 2
    assert len(results.succeeded) == 1
    assert len(results.skipped) == 1
