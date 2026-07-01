import datetime
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from cloudpathlib import GSPath

from gfw.ops.pipelines.compact_parquet.main import Compactor, _date_range, _duckdb_conn, run


DATE = datetime.date(2024, 1, 1)
GCS_PATH = GSPath("gs://bucket/path/messages")
COPY_STAGING_PATH = GSPath("gs://bucket/path/compacted_messages")


def _make_blob(name: str, size: int = 10 * 1024 * 1024) -> MagicMock:
    blob = MagicMock()
    blob.name = name
    blob.size = size
    blob.bucket.name = "bucket"
    return blob


def _source_blobs(n: int = 3) -> list[MagicMock]:
    return [
        _make_blob(f"path/messages/event_source=src/event_date=2024-01-01/part_{i}.parquet")
        for i in range(n)
    ]


def _staging_blobs(n: int = 1) -> list[MagicMock]:
    return [
        _make_blob(
            f"path/_compact_messages_staging/event_source=src/"
            f"event_date=2024-01-01/part_{i}.parquet"
        )
        for i in range(n)
    ]


def _make_compactor(
    source_blobs=None,
    staging_blobs_sequence=None,
    conn_factory=None,
    gcs_staging_path=None,
) -> Compactor:
    """Return a Compactor with mocked GCS client and DuckDB connection factory.

    staging_blobs_sequence is a list of blob lists consumed in order for each
    staging-path list_blobs call. Defaults to [] (always returns empty).
    Defaults to swap mode. Pass gcs_staging_path to get copy mode.
    """
    source_blobs = source_blobs if source_blobs is not None else _source_blobs()
    staging_iter = iter(staging_blobs_sequence or [])

    mock_gcs = MagicMock()

    def list_blobs(bucket, prefix):
        if prefix.startswith(GCS_PATH.blob):
            return iter(source_blobs)
        return iter(next(staging_iter, []))

    mock_gcs.list_blobs.side_effect = list_blobs

    return Compactor(
        gcs_client=mock_gcs,
        gcs_output_path=GCS_PATH,
        event_source="src",
        partition_prefix="event_",
        target_file_size_mb=512,
        gcs_staging_path=gcs_staging_path,
        conn_factory=conn_factory or MagicMock,
    )


# --- _duckdb_conn ---


def test_duckdb_conn_configures_connection():
    mock_conn = MagicMock()
    with patch(
        "gfw.ops.pipelines.compact_parquet.main.duckdb.connect", return_value=mock_conn
    ) as mock_connect:
        with patch("gfw.ops.pipelines.compact_parquet.main.gcsfs.GCSFileSystem") as mock_fs:
            conn = _duckdb_conn(memory_limit=4, threads=2)

    assert conn is mock_conn
    mock_connect.assert_called_once_with(
        config={
            "memory_limit": "4GB",
            "threads": 2,
            "preserve_insertion_order": False,
        }
    )
    mock_conn.register_filesystem.assert_called_once_with(mock_fs.return_value)


# --- _date_range ---


def test_date_range_basic():
    dates = _date_range("2024-01-01", "2024-01-04")
    assert dates == [
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
    ]


def test_date_range_empty():
    assert _date_range("2024-01-01", "2024-01-01") == []


# --- skip logic ---


def test_compact_skips_when_no_source_files():
    compactor = _make_compactor(source_blobs=[])
    compactor._compact(DATE)  # no error = no write attempted


def test_compact_skips_when_already_single_file():
    compactor = _make_compactor(source_blobs=_source_blobs(1))
    compactor._compact(DATE)  # no error = no write attempted


# --- normal compaction flow ---


def test_compact_swap_full_flow():
    """write staging → delete source → copy staging → delete staging."""
    source = _source_blobs(3)
    staged = _staging_blobs(1)
    # 1st staging call (existing-check): empty; 2nd (post-write list): staged
    compactor = _make_compactor(source_blobs=source, staging_blobs_sequence=[[], staged])

    compactor._compact(DATE)

    for blob in source:
        blob.delete.assert_called_once()
    staged[0].delete.assert_called_once()
    compactor.gcs_client.bucket.return_value.copy_blob.assert_called_once()


def test_compact_deletes_source_before_copy():
    """Source blobs must be deleted before the copy step."""
    source = _source_blobs(3)
    staged = _staging_blobs(1)
    compactor = _make_compactor(source_blobs=source, staging_blobs_sequence=[[], staged])
    ops = []
    source[0].delete.side_effect = lambda: ops.append("delete")
    compactor.gcs_client.bucket.return_value.copy_blob.side_effect = lambda *a, **k: ops.append(
        "copy"
    )

    compactor._compact(DATE)

    assert ops.index("delete") < ops.index("copy")


# --- resume interrupted swap ---


def test_compact_resumes_interrupted_swap():
    """If staging exists but source is gone, copy staging and clean up."""
    staged = _staging_blobs(1)
    compactor = _make_compactor(source_blobs=[], staging_blobs_sequence=[staged])

    compactor._compact(DATE)

    compactor.gcs_client.bucket.return_value.copy_blob.assert_called_once()
    staged[0].delete.assert_called_once()


# --- leftover staging cleanup ---


def test_compact_cleans_up_leftover_staging_before_writing():
    """Leftover staging files from a prior run are deleted before writing new ones."""
    leftover = _staging_blobs(2)
    # 1st staging call: leftover exists; 2nd (post-write): fresh empty result
    compactor = _make_compactor(staging_blobs_sequence=[leftover, []])

    compactor._compact(DATE)

    for blob in leftover:
        blob.delete.assert_called_once()


# --- DuckDB SQL generation ---


def test_write_compacted_executes_copy_sql():
    mock_conn = MagicMock()
    compactor = _make_compactor(conn_factory=MagicMock(return_value=mock_conn))

    source_uris = [
        "gs://bucket/path/messages/event_source=src/event_date=2024-01-01/part_0.parquet"
    ]

    compactor._write_compacted(DATE, source_uris, compactor.gcs_staging_path)

    executed_sql = compactor.connection.execute.call_args[0][0]
    assert "COPY" in executed_sql
    assert "FORMAT PARQUET" in executed_sql
    assert "COMPRESSION SNAPPY" in executed_sql
    assert source_uris[0] in executed_sql
    assert str(512 * 1024 * 1024) in executed_sql


def test_run_closes_connection():
    mock_conn = MagicMock()
    compactor = _make_compactor(conn_factory=MagicMock(return_value=mock_conn))
    compactor.run([DATE])
    mock_conn.close.assert_called_once()


def test_close_connection_noop_if_never_opened():
    compactor = _make_compactor()
    compactor.close_connection()  # no error, no connection created


# --- copy mode ---


def test_compactor_copy_mode_does_not_set_swap():
    compactor = _make_compactor(gcs_staging_path=COPY_STAGING_PATH)
    assert compactor.swap is False
    assert compactor.gcs_staging_path == COPY_STAGING_PATH


def test_compact_copy_mode_does_not_touch_source():
    """In copy mode, source files are left untouched after compaction."""
    source = _source_blobs(3)
    compactor = _make_compactor(source_blobs=source, gcs_staging_path=COPY_STAGING_PATH)

    compactor._compact(DATE)

    for blob in source:
        blob.delete.assert_not_called()
    compactor.gcs_client.bucket.return_value.copy_blob.assert_not_called()


# --- retry ---


def test_compact_retries_on_io_error():
    """A transient IOException triggers a retry; connection is reset between attempts."""
    calls = 0

    def flaky_conn(**kwargs):
        conn = MagicMock()
        nonlocal calls
        calls += 1
        if calls == 1:
            conn.execute.side_effect = duckdb.IOException("transient")
        return conn

    source = _source_blobs(3)
    staged = _staging_blobs(1)
    compactor = _make_compactor(
        source_blobs=source,
        staging_blobs_sequence=[[], [], staged],
        conn_factory=flaky_conn,
    )

    compactor._compact_with_retry(DATE)

    assert calls == 2


def test_compact_reraises_after_max_retries():
    """Exhausting all retries re-raises the last exception."""
    conn = MagicMock()
    conn.execute.side_effect = duckdb.IOException("persistent")
    compactor = _make_compactor(
        staging_blobs_sequence=[[], [], [], []],
        conn_factory=lambda **kwargs: conn,
    )
    compactor.max_retries = 2

    with pytest.raises(duckdb.IOException):
        compactor._compact_with_retry(DATE)


# --- Compactor.run() ---


def test_run_processes_all_dates():
    compactor = _make_compactor()
    compactor.run([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)])

    prefixes = [call.kwargs["prefix"] for call in compactor.gcs_client.list_blobs.call_args_list]
    assert any("2024-01-01" in p for p in prefixes)
    assert any("2024-01-02" in p for p in prefixes)


# --- _copy_to_partition and _delete_blobs ---


def test_copy_to_partition_calls_gcs_copy():
    compactor = _make_compactor()
    blob = _make_blob("path/messages/event_source=src/event_date=2024-01-01/part_0.parquet")

    compactor._copy_to_partition(DATE, [blob], GCS_PATH)

    bucket = compactor.gcs_client.bucket.return_value
    expected_dest = "path/messages/event_source=src/event_date=2024-01-01/part_0.parquet"
    bucket.copy_blob.assert_called_once_with(blob, bucket, new_name=expected_dest)


def test_delete_blobs_deletes_each_blob():
    compactor = _make_compactor()
    blobs = [_make_blob(f"path/part_{i}.parquet") for i in range(3)]

    compactor._delete_blobs(blobs)

    for blob in blobs:
        blob.delete.assert_called_once()


# --- dry run ---


def test_run_dry_run_makes_no_gcs_calls():
    mock_gcs = MagicMock()
    run(
        project="proj",
        gcs_output_path="gs://bucket/messages",
        event_source="src",
        start_date="2024-01-01",
        end_date="2024-01-02",
        dry_run=True,
        gcs_client_factory=lambda project: mock_gcs,
    )
    mock_gcs.list_blobs.assert_not_called()


# --- run() non-dry-run ---


def test_run_function_runs_compactor():
    mock_gcs = MagicMock()
    mock_gcs.list_blobs.return_value = iter([])

    run(
        project="proj",
        gcs_output_path="gs://bucket/messages",
        event_source="src",
        start_date="2024-01-01",
        end_date="2024-01-02",
        gcs_client_factory=lambda project: mock_gcs,
        conn_factory=MagicMock,
    )

    mock_gcs.list_blobs.assert_called()
