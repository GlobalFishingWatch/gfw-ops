from unittest.mock import MagicMock, patch

import pytest

from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery

from gfw.ops.pipelines.sharded_to_partitioned.main import ShardedToPartitioned, Table


def _make_stp(schema=None):
    return ShardedToPartitioned(
        tables=["proj.ds.table_a", "proj.ds.table_b"],
        target="proj.ds.target",
        project="proj",
        schema=schema or [],
        start_date="2023-01-01",
        end_date="2023-03-01",
        bq_client_factory=MagicMock(),
    )


# --- Table ---


def test_table_from_fully_qualified():
    t = Table.from_fully_qualified("my-project.my_dataset.my_table")
    assert t.project == "my-project"
    assert t.dataset_id == "my_dataset"
    assert t.table_id == "my_table"


def test_table_str_and_properties():
    t = Table(project="p", dataset_id="d", table_id="t")
    assert str(t) == "p.d.t"
    assert t.fully_qualified == "p.d.t"
    assert t.dataset == "p.d"


def test_table_unpack():
    project, dataset, table = Table(project="p", dataset_id="d", table_id="t")
    assert (project, dataset, table) == ("p", "d", "t")


# --- ShardedToPartitioned._iter_months ---


def test_iter_months():
    assert ShardedToPartitioned._iter_months("2023-01-01", "2023-04-01") == [
        "2023-01",
        "2023-02",
        "2023-03",
    ]


def test_iter_months_year_boundary():
    assert ShardedToPartitioned._iter_months("2022-11-01", "2023-02-01") == [
        "2022-11",
        "2022-12",
        "2023-01",
    ]


def test_iter_months_empty_when_start_equals_end():
    assert ShardedToPartitioned._iter_months("2023-01-01", "2023-01-01") == []


def test_iter_months_partial_start_includes_that_month():
    assert ShardedToPartitioned._iter_months("2023-01-15", "2023-03-01") == [
        "2023-01",
        "2023-02",
    ]


def test_iter_months_partial_end_includes_that_month():
    assert ShardedToPartitioned._iter_months("2023-01-01", "2023-03-15") == [
        "2023-01",
        "2023-02",
        "2023-03",
    ]


# --- ShardedToPartitioned._build_query ---


def test_build_query_includes_all_tables_for_month():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)
    table_columns = {
        "proj.ds.table_a": frozenset(["ts"]),
        "proj.ds.table_b": frozenset(["ts"]),
    }

    query = stp._build_query("2023-01", table_columns)

    assert "proj.ds.table_a_*" in query
    assert "proj.ds.table_b_*" in query
    assert "_TABLE_SUFFIX >= '20230101' AND _TABLE_SUFFIX < '20230201'" in query
    assert "UNION ALL" in query


def test_build_query_null_cast_for_missing_column():
    schema = [
        bigquery.SchemaField("ts", "TIMESTAMP"),
        bigquery.SchemaField("msg", "STRING"),
    ]
    stp = _make_stp(schema=schema)
    table_columns = {
        "proj.ds.table_a": frozenset(["ts", "msg"]),
        "proj.ds.table_b": frozenset(["ts"]),  # missing "msg"
    }

    query = stp._build_query("2023-01", table_columns)

    # only table_b is missing "msg", so exactly one NULL cast should appear
    assert query.count("CAST(NULL AS STRING) AS msg") == 1


def test_build_query_december_wraps_year():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = ShardedToPartitioned(
        tables=["proj.ds.table_a", "proj.ds.table_b"],
        target="proj.ds.target",
        project="proj",
        schema=schema,
        start_date="2022-12-01",
        end_date="2023-01-01",
        bq_client_factory=MagicMock(),
    )
    table_columns = {"proj.ds.table_a": frozenset(["ts"]), "proj.ds.table_b": frozenset(["ts"])}

    query = stp._build_query("2022-12", table_columns)

    assert "_TABLE_SUFFIX >= '20221201' AND _TABLE_SUFFIX < '20230101'" in query


def test_build_query_clamps_suffix_to_start_date():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = ShardedToPartitioned(
        tables=["proj.ds.table_a"],
        target="proj.ds.target",
        project="proj",
        schema=schema,
        start_date="2023-01-15",
        end_date="2023-02-01",
        bq_client_factory=MagicMock(),
    )
    query = stp._build_query("2023-01", {"proj.ds.table_a": frozenset(["ts"])})
    assert "_TABLE_SUFFIX >= '20230115'" in query
    assert "_TABLE_SUFFIX < '20230201'" in query


def test_build_query_clamps_suffix_to_end_date():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = ShardedToPartitioned(
        tables=["proj.ds.table_a"],
        target="proj.ds.target",
        project="proj",
        schema=schema,
        start_date="2023-01-01",
        end_date="2023-01-22",
        bq_client_factory=MagicMock(),
    )
    query = stp._build_query("2023-01", {"proj.ds.table_a": frozenset(["ts"])})
    assert "_TABLE_SUFFIX >= '20230101'" in query
    assert "_TABLE_SUFFIX < '20230122'" in query


# --- ShardedToPartitioned properties ---


def test_schema_from_list():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = ShardedToPartitioned(
        tables=["proj.ds.t"],
        target="proj.ds.target",
        project="proj",
        schema=schema,
        start_date="2023-01-01",
        end_date="2023-02-01",
        bq_client_factory=MagicMock(),
    )
    assert stp.schema == schema


def test_partition_type():
    stp = _make_stp()
    assert stp.partition_type == bigquery.TimePartitioningType.DAY


def test_partition_type_invalid_raises():
    stp = ShardedToPartitioned(
        tables=["proj.ds.t"],
        target="proj.ds.target",
        project="proj",
        schema=[],
        start_date="2023-01-01",
        end_date="2023-02-01",
        partition_type="WEEK",
        bq_client_factory=MagicMock(),
    )
    with pytest.raises(ValueError, match="Invalid partition_type"):
        _ = stp.partition_type


# --- ShardedToPartitioned._ensure_table ---


def test_ensure_table():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)
    stp._ensure_table()
    _, kwargs = stp.client.create_table.call_args
    assert kwargs["exists_ok"] is True


# --- ShardedToPartitioned._compute_pending ---


def test_compute_pending_not_found_returns_all_months():
    stp = _make_stp()
    stp.client.query.side_effect = NotFound("table not found")
    months = ["2023-01", "2023-02"]

    result = stp._compute_pending(months)

    assert result == months


def test_compute_pending_skips_existing_months():
    stp = _make_stp()
    stp.client.query.return_value.result.return_value = [
        type("Row", (), {"partition_id": "20230101"})(),
        type("Row", (), {"partition_id": "20230115"})(),
    ]
    months = ["2023-01", "2023-02"]

    result = stp._compute_pending(months)

    assert result == ["2023-02"]


# --- ShardedToPartitioned._process_month ---


def test_process_month():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)
    table_columns = {"proj.ds.table_a": frozenset(["ts"]), "proj.ds.table_b": frozenset(["ts"])}
    stp.client.query.return_value.total_bytes_processed = 0

    stp._process_month("2023-01", table_columns, overwrite=False)

    stp.client.query.assert_called_once()


def test_process_month_overwrite_deletes_first():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)
    table_columns = {"proj.ds.table_a": frozenset(["ts"]), "proj.ds.table_b": frozenset(["ts"])}
    stp.client.query.return_value.total_bytes_processed = 0

    stp._process_month("2023-01", table_columns, overwrite=True)

    assert stp.client.query.call_count == 2  # delete + insert


def test_process_month_google_api_error_returns_false():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)
    table_columns = {"proj.ds.table_a": frozenset(["ts"]), "proj.ds.table_b": frozenset(["ts"])}
    stp.client.query.return_value.result.side_effect = GoogleAPIError("BQ error")

    assert stp._process_month("2023-01", table_columns, overwrite=False) is False


def test_run_overwrite_skips_compute_pending():
    stp = _make_stp()

    with (
        patch.object(stp, "_compute_pending") as mock_pending,
        patch.object(stp, "_discover_columns", return_value={}),
        patch.object(stp, "_ensure_table"),
        patch.object(stp, "_process_month", return_value=True),
    ):
        stp.run(overwrite=True)

    mock_pending.assert_not_called()


def test_run_limit_caps_months_processed():
    stp = _make_stp()

    with (
        patch.object(stp, "_compute_pending", return_value=["2023-01", "2023-02"]),
        patch.object(stp, "_discover_columns", return_value={}),
        patch.object(stp, "_ensure_table"),
        patch.object(stp, "_process_month", return_value=True) as mock_pm,
    ):
        stp.run(limit=1)

    assert mock_pm.call_count == 1


def test_run_logs_skipped_months():
    stp = _make_stp()

    with (
        patch.object(stp, "_compute_pending", return_value=["2023-02"]),
        patch.object(stp, "_discover_columns", return_value={}),
        patch.object(stp, "_ensure_table"),
        patch.object(stp, "_process_month", return_value=True),
    ):
        stp.run()  # 2 months in range, 1 pending → 1 skipped


def test_run_raises_after_all_months_attempted_when_some_fail():
    schema = [bigquery.SchemaField("ts", "TIMESTAMP")]
    stp = _make_stp(schema=schema)

    with (
        patch.object(stp, "_compute_pending", return_value=["2023-01", "2023-02"]),
        patch.object(stp, "_discover_columns", return_value={}),
        patch.object(stp, "_ensure_table"),
        patch.object(stp, "_process_month", return_value=False) as mock_pm,
    ):
        with pytest.raises(RuntimeError, match="failed month"):
            stp.run()

    # Both months were attempted despite the first one failing
    assert mock_pm.call_count == 2
