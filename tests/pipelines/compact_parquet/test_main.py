import datetime
from unittest.mock import MagicMock, patch

from gfw.ops.pipelines.compact_parquet.main import Compactor, _date_range, run
from cloudpathlib import GSPath


DATE = datetime.date(2024, 1, 1)
GCS_PATH = GSPath("gs://bucket/path/messages")
STAGING_PATH = GSPath("gs://bucket/path/_compact_messages_staging")


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
    staging_blobs=None,
    conn_factory=None,
) -> Compactor:
    """Return a Compactor with mocked GCS client and DuckDB connection."""
    source_blobs = source_blobs if source_blobs is not None else _source_blobs()
    staging_blobs = staging_blobs if staging_blobs is not None else []

    mock_gcs = MagicMock()

    def list_blobs(bucket, prefix):
        if "_staging" in prefix:
            return iter(staging_blobs)
        return iter(source_blobs)

    mock_gcs.list_blobs.side_effect = list_blobs

    if conn_factory is None:
        def conn_factory():
            return MagicMock()

    return Compactor(
        gcs_client=mock_gcs,
        gcs_path=GCS_PATH,
        staging_path=STAGING_PATH,
        event_source="src",
        partition_prefix="event_",
        target_file_size_mb=512,
        conn_factory=conn_factory,
    )


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
    compactor = _make_compactor(source_blobs=[], staging_blobs=[])
    with patch.object(compactor, "_write_compacted_to_staging") as mock_write:
        compactor._compact(DATE)
        mock_write.assert_not_called()


def test_compact_skips_when_already_single_file():
    compactor = _make_compactor(source_blobs=_source_blobs(1))
    with patch.object(compactor, "_write_compacted_to_staging") as mock_write:
        compactor._compact(DATE)
        mock_write.assert_not_called()


# --- normal compaction flow ---


def test_compact_normal_flow_order():
    """write staging → delete source → copy staging → delete staging."""
    staged = _staging_blobs(1)
    compactor = _make_compactor()
    calls = []

    with patch.object(
        compactor, "_write_compacted_to_staging", return_value=staged
    ) as mock_write:
        with patch.object(
            compactor, "_delete_blobs", side_effect=lambda b: calls.append(("delete", b))
        ):
            with patch.object(
                compactor, "_copy_to_partition", side_effect=lambda d, b: calls.append(("copy", b))
            ):
                compactor._compact(DATE)

    mock_write.assert_called_once()
    assert calls[0][0] == "delete"  # source deleted first
    assert calls[1][0] == "copy"    # then staging copied
    assert calls[2][0] == "delete"  # then staging deleted


def test_compact_deletes_source_before_copy():
    """Source blobs must be deleted before the copy step."""
    source = _source_blobs(3)
    staged = _staging_blobs(1)
    compactor = _make_compactor(source_blobs=source)
    deleted = []

    with patch.object(compactor, "_write_compacted_to_staging", return_value=staged):
        with patch.object(compactor, "_delete_blobs", side_effect=lambda b: deleted.append(b)):
            with patch.object(compactor, "_copy_to_partition"):
                compactor._compact(DATE)

    assert deleted[0] == source


# --- resume interrupted swap ---


def test_compact_resumes_interrupted_swap():
    """If staging exists but source is gone, copy staging and clean up."""
    staged = _staging_blobs(1)
    compactor = _make_compactor(source_blobs=[], staging_blobs=staged)

    with patch.object(compactor, "_copy_to_partition") as mock_copy:
        with patch.object(compactor, "_delete_blobs") as mock_delete:
            compactor._compact(DATE)

    mock_copy.assert_called_once_with(DATE, staged)
    mock_delete.assert_called_once_with(staged)


# --- leftover staging cleanup ---


def test_compact_cleans_up_leftover_staging_before_writing():
    """Leftover staging files from a prior run are deleted before writing new ones."""
    leftover = _staging_blobs(2)
    staged = _staging_blobs(1)
    compactor = _make_compactor(staging_blobs=leftover)
    deleted = []

    with patch.object(compactor, "_write_compacted_to_staging", return_value=staged):
        with patch.object(compactor, "_delete_blobs", side_effect=lambda b: deleted.append(b)):
            with patch.object(compactor, "_copy_to_partition"):
                compactor._compact(DATE)

    assert deleted[0] == leftover  # leftover deleted first


# --- DuckDB SQL generation ---


def test_write_compacted_to_staging_executes_copy_sql():
    mock_conn = MagicMock()
    compactor = _make_compactor(conn_factory=lambda: mock_conn)

    source_uris = [
        "gs://bucket/path/messages/event_source=src/event_date=2024-01-01/part_0.parquet"
    ]

    with patch.object(compactor, "_list_parquet_blobs", return_value=_staging_blobs(1)):
        compactor._write_compacted_to_staging(DATE, source_uris)

    executed_sql = mock_conn.execute.call_args[0][0]
    assert "COPY" in executed_sql
    assert "FORMAT PARQUET" in executed_sql
    assert "COMPRESSION SNAPPY" in executed_sql
    assert source_uris[0] in executed_sql
    assert str(512 * 1024 * 1024) in executed_sql


def test_write_compacted_to_staging_closes_connection():
    mock_conn = MagicMock()
    compactor = _make_compactor(conn_factory=lambda: mock_conn)

    with patch.object(compactor, "_list_parquet_blobs", return_value=_staging_blobs(1)):
        compactor._write_compacted_to_staging(DATE, ["gs://bucket/x/part.parquet"])

    mock_conn.close.assert_called_once()


# --- dry run ---


def test_run_dry_run_makes_no_gcs_calls():
    mock_gcs = MagicMock()
    run(
        project="proj",
        gcs_path="gs://bucket/messages",
        event_source="src",
        start_date="2024-01-01",
        end_date="2024-01-03",
        dry_run=True,
        gcs_client_factory=lambda project: mock_gcs,
    )
    mock_gcs.list_blobs.assert_not_called()
