import datetime
import json

from functools import partial
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from cloudpathlib import GSPath

from gfw.ops.pipelines.compact_parquet.main import (
    Compactor,
    _date_range,
    _duckdb_conn,
    _httpfs_duckdb_conn,
    run,
)
from gfw.ops.pipelines.compact_parquet.units import DailyCompactionUnit, HourlyCompactionUnit


DATE = datetime.date(2024, 1, 1)
HOUR = "05"
UNIT = HourlyCompactionUnit(DATE, HOUR)
FLAT_UNIT = DailyCompactionUnit(DATE)
GCS_PATH = GSPath("gs://bucket/path/messages")
COPY_STAGING_PATH = GSPath("gs://bucket/path/compacted_messages")


class _FakeIterator:
    """Mimics a GCS HTTPIterator: iterable over blobs, with `.prefixes` populated
    (in the real API, only after the iterator has been consumed) when a delimiter
    listing is used for hive-subpartition discovery.
    """

    def __init__(self, blobs=(), prefixes=()):
        self._blobs = list(blobs)
        self.prefixes = set(prefixes)

    def __iter__(self):
        return iter(self._blobs)


def _make_blob(name: str, size: int = 10 * 1024 * 1024) -> MagicMock:
    blob = MagicMock()
    blob.name = name
    blob.size = size
    blob.bucket.name = "bucket"
    return blob


def _source_blobs(n: int = 3, hour: str = HOUR) -> list[MagicMock]:
    return [
        _make_blob(
            f"path/messages/event_source=src/event_date=2024-01-01/"
            f"event_hour={hour}/part_{i}.parquet"
        )
        for i in range(n)
    ]


def _staging_blobs(n: int = 1, hour: str = HOUR) -> list[MagicMock]:
    return [
        _make_blob(
            f"path/_compact_messages_staging/event_source=src/"
            f"event_date=2024-01-01/event_hour={hour}/part_{i}.parquet"
        )
        for i in range(n)
    ]


def _flat_source_blobs(n: int = 3) -> list[MagicMock]:
    return [
        _make_blob(f"path/messages/event_source=src/event_date=2024-01-01/part_{i}.parquet")
        for i in range(n)
    ]


def _flat_staging_blobs(n: int = 1) -> list[MagicMock]:
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
    manifest=None,
    hours=(HOUR,),
    hourly=True,
) -> Compactor:
    """Return a Compactor with mocked GCS client and DuckDB connection factory.

    staging_blobs_sequence is a list of blob lists consumed in order for each
    staging-path list_blobs call. Defaults to [] (always returns empty).
    Defaults to swap mode. Pass gcs_staging_path to get copy mode.
    Pass manifest (a list of blob names) to simulate a pre-existing manifest file.
    hours controls which {prefix}hour= subpartitions `_list_hours` discovers.
    hourly defaults to True here (unlike Compactor's own default of False) since most
    of these tests exercise the hourly compaction path built for hive-partitioned sources.
    """
    source_blobs = source_blobs if source_blobs is not None else _source_blobs()
    staging_iter = iter(staging_blobs_sequence or [])

    mock_gcs = MagicMock()

    def list_blobs(bucket, prefix, delimiter=None):
        if delimiter is not None:
            return _FakeIterator(prefixes={f"{prefix}event_hour={h}/" for h in hours})
        if prefix.startswith(GCS_PATH.blob):
            return iter(source_blobs)
        return iter(next(staging_iter, []))

    mock_gcs.list_blobs.side_effect = list_blobs

    manifest_blob = mock_gcs.bucket.return_value.blob.return_value
    manifest_blob.exists.return_value = manifest is not None
    if manifest is not None:
        manifest_blob.download_as_text.return_value = json.dumps(manifest)

    return Compactor(
        gcs_client=mock_gcs,
        gcs_input_path=GCS_PATH,
        event_source="src",
        partition_prefix="event_",
        target_file_size_mb=512,
        gcs_staging_path=gcs_staging_path,
        hourly=hourly,
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


# --- _httpfs_duckdb_conn ---


def test_httpfs_duckdb_conn_configures_connection():
    mock_conn = MagicMock()
    with patch(
        "gfw.ops.pipelines.compact_parquet.main.duckdb.connect", return_value=mock_conn
    ) as mock_connect:
        conn = _httpfs_duckdb_conn(
            key_id="AKIAKEY", secret="shh-secret", memory_limit=4, threads=2
        )

    assert conn is mock_conn
    mock_connect.assert_called_once_with(
        config={
            "memory_limit": "4GB",
            "threads": 2,
            "preserve_insertion_order": False,
            "http_timeout": 300000,
            "http_retries": 5,
        }
    )
    mock_conn.install_extension.assert_called_once_with("httpfs")
    mock_conn.load_extension.assert_called_once_with("httpfs")
    secret_stmt = mock_conn.execute.call_args[0][0]
    assert "CREATE SECRET" in secret_stmt
    assert "AKIAKEY" in secret_stmt
    assert "shh-secret" in secret_stmt
    assert "TYPE gcs" in secret_stmt


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
    compactor._compact(UNIT, MagicMock())  # no error = no write attempted


def test_compact_skips_when_already_single_file():
    compactor = _make_compactor(source_blobs=_source_blobs(1))
    compactor._compact(UNIT, MagicMock())  # no error = no write attempted


# --- normal compaction flow ---


def test_compact_swap_full_flow():
    """Write staging → delete source → copy staging → delete staging."""
    source = _source_blobs(3)
    staged = _staging_blobs(1)
    # 1st staging call (existing-check): empty; 2nd (post-write list): staged
    compactor = _make_compactor(source_blobs=source, staging_blobs_sequence=[[], staged])

    compactor._compact(UNIT, MagicMock())

    bucket = compactor.gcs_client.bucket.return_value
    deleted_names = [
        call.args[0] for call in bucket.blob.call_args_list if not call.args[0].endswith(".json")
    ]
    assert set(deleted_names) == {b.name for b in source}
    staged[0].delete.assert_called_once()
    bucket.copy_blob.assert_called_once()


def test_compact_deletes_source_before_copy():
    """Source blobs must be deleted before the copy step."""
    source = _source_blobs(3)
    staged = _staging_blobs(1)
    compactor = _make_compactor(source_blobs=source, staging_blobs_sequence=[[], staged])
    ops = []
    bucket = compactor.gcs_client.bucket.return_value
    bucket.blob.return_value.delete.side_effect = lambda: ops.append("delete")
    bucket.copy_blob.side_effect = lambda *a, **k: ops.append("copy")

    compactor._compact(UNIT, MagicMock())

    assert ops.index("delete") < ops.index("copy")


# --- resume interrupted swap ---


def test_compact_raises_on_ambiguous_state_without_manifest():
    """Staging non-empty, source empty, no manifest: unverifiable state — must not
    auto-recover. Refuse and surface the problem instead of guessing.
    """
    staged = _staging_blobs(1)
    compactor = _make_compactor(source_blobs=[], staging_blobs_sequence=[staged])

    with pytest.raises(RuntimeError, match="Ambiguous state"):
        compactor._compact(UNIT, MagicMock())

    compactor.gcs_client.bucket.return_value.copy_blob.assert_not_called()
    staged[0].delete.assert_not_called()


def test_compact_resumes_committed_swap_via_manifest():
    """A manifest takes priority over directory state: finish deleting exactly the
    recorded originals and copy back, even though the live partition currently shows
    only a partial remnant (2 of 3 originals already deleted before a crash).
    """
    all_originals = _source_blobs(3)
    manifest_names = [b.name for b in all_originals]
    surviving_remnant = all_originals[:1]
    staged = _staging_blobs(1)

    compactor = _make_compactor(
        source_blobs=surviving_remnant,
        staging_blobs_sequence=[staged],
        manifest=manifest_names,
    )
    mock_connection = MagicMock()

    compactor._compact(UNIT, mock_connection)

    bucket = compactor.gcs_client.bucket.return_value
    deleted_names = [
        call.args[0] for call in bucket.blob.call_args_list if call.args[0] in manifest_names
    ]
    assert set(deleted_names) == set(manifest_names)
    bucket.copy_blob.assert_called_once()
    staged[0].delete.assert_called_once()
    mock_connection.execute.assert_not_called()  # never recompacted the surviving remnant


def test_write_and_read_manifest_round_trip():
    compactor = _make_compactor()

    compactor._write_manifest(UNIT, ["a.parquet", "b.parquet"])

    bucket = compactor.gcs_client.bucket.return_value
    written_json = bucket.blob.return_value.upload_from_string.call_args[0][0]
    assert json.loads(written_json) == ["a.parquet", "b.parquet"]


def test_read_manifest_returns_none_when_absent():
    compactor = _make_compactor()
    assert compactor._read_manifest(UNIT) is None


def test_read_manifest_returns_recorded_names():
    names = ["a.parquet", "b.parquet"]
    compactor = _make_compactor(manifest=names)
    assert compactor._read_manifest(UNIT) == names


def test_delete_named_blobs_tolerates_already_deleted():
    """A blob already removed by a partial prior delete (404 on retry) is a no-op."""
    from google.api_core.exceptions import NotFound

    compactor = _make_compactor()
    bucket = compactor.gcs_client.bucket.return_value
    bucket.blob.return_value.delete.side_effect = [None, NotFound("gone"), None]

    compactor._delete_named_blobs(GCS_PATH, ["a.parquet", "b.parquet", "c.parquet"])

    assert bucket.blob.return_value.delete.call_count == 3


def test_delete_manifest_tolerates_already_deleted():
    from google.api_core.exceptions import NotFound

    compactor = _make_compactor()
    bucket = compactor.gcs_client.bucket.return_value
    bucket.blob.return_value.delete.side_effect = NotFound("gone")

    compactor._delete_manifest(UNIT)  # no error raised


# --- leftover staging cleanup ---


def test_compact_cleans_up_leftover_staging_before_writing():
    """Leftover staging files from a prior run are deleted before writing new ones."""
    leftover = _staging_blobs(2)
    # 1st staging call: leftover exists; 2nd (post-write): fresh empty result
    compactor = _make_compactor(staging_blobs_sequence=[leftover, []])

    compactor._compact(UNIT, MagicMock())

    for blob in leftover:
        blob.delete.assert_called_once()


# --- DuckDB SQL generation ---


def test_write_compacted_executes_copy_sql():
    mock_conn = MagicMock()
    compactor = _make_compactor(conn_factory=MagicMock(return_value=mock_conn))

    source_uris = [
        "gs://bucket/path/messages/event_source=src/event_date=2024-01-01/"
        "event_hour=05/part_0.parquet"
    ]
    dest_part = UNIT.path(
        compactor.gcs_staging_path, compactor.event_source, compactor.partition_prefix
    )

    compactor._write_compacted(mock_conn, dest_part, source_uris)

    executed_sql = mock_conn.execute.call_args[0][0]
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


# --- copy mode ---


def test_compactor_copy_mode_does_not_set_swap():
    compactor = _make_compactor(gcs_staging_path=COPY_STAGING_PATH)
    assert compactor.swap is False
    assert compactor.gcs_staging_path == COPY_STAGING_PATH


def test_compact_copy_mode_does_not_touch_source():
    """In copy mode, source files are left untouched after compaction."""
    source = _source_blobs(3)
    compactor = _make_compactor(source_blobs=source, gcs_staging_path=COPY_STAGING_PATH)

    compactor._compact(UNIT, MagicMock())

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

    compactor._compact_unit(UNIT)

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
        compactor._compact_unit(UNIT)


# --- CompactionUnit.path() ---


def test_daily_compaction_unit_path_has_no_hour_segment():
    unit = DailyCompactionUnit(DATE)
    path = unit.path(GCS_PATH, "src", "event_")
    assert str(path) == "gs://bucket/path/messages/event_source=src/event_date=2024-01-01"


def test_hourly_compaction_unit_path_appends_hour_segment():
    unit = HourlyCompactionUnit(DATE, HOUR)
    path = unit.path(GCS_PATH, "src", "event_")
    assert str(path) == (
        "gs://bucket/path/messages/event_source=src/event_date=2024-01-01/event_hour=05"
    )


def test_daily_compaction_unit_str_has_no_hour():
    assert str(DailyCompactionUnit(DATE)) == "2024-01-01"


def test_hourly_compaction_unit_str_includes_hour():
    assert str(HourlyCompactionUnit(DATE, HOUR)) == "2024-01-01 hour=05"


def test_daily_compaction_unit_with_hour_derives_hourly_unit_for_same_date():
    daily = DailyCompactionUnit(DATE)
    assert daily.with_hour(HOUR) == HourlyCompactionUnit(DATE, HOUR)


# --- hour-partition discovery ---


def test_list_hours_discovers_and_sorts_hour_partitions():
    compactor = _make_compactor(hours=("12", "00", "05"))
    assert compactor._list_hours(DailyCompactionUnit(DATE)) == ["00", "05", "12"]


def test_list_hours_empty_when_no_hour_subpartitions():
    compactor = _make_compactor(hours=())
    assert compactor._list_hours(DailyCompactionUnit(DATE)) == []


# --- hourly vs flat partitioning (declared ahead of time, not auto-detected) ---


def test_units_for_flat_mode_returns_single_whole_date_unit():
    """hourly=False: a date with no hour subpartitions compacts as one whole-date unit."""
    compactor = _make_compactor(hours=(), hourly=False)
    assert compactor._units_for(DATE) == [DailyCompactionUnit(DATE)]


def test_units_for_preserves_hour_subpartitions_even_when_hourly_is_false():
    """Hourly is not a mode switch: hour subpartitions are always preserved when found,
    regardless of the flag — collapsing a real hour partition just because the caller's
    config was stale or wrong would break the external table's hive partitioning.
    """
    compactor = _make_compactor(hours=(HOUR,), hourly=False)
    assert compactor._units_for(DATE) == [HourlyCompactionUnit(DATE, HOUR)]


def test_units_for_hourly_mode_raises_when_no_hours_found():
    """A date configured as hourly but with no hour subpartitions at all — whether it's
    actually flat (predates a pipeline's move to hourly output) or ingestion for this
    date simply never completed — is refused rather than silently skipped or compacted
    at the date level: by the time compaction runs, the date is expected to be full.
    """
    compactor = _make_compactor(hours=(), hourly=True)
    with pytest.raises(RuntimeError, match="hourly=True"):
        compactor._units_for(DATE)


def test_units_for_hourly_mode_returns_one_unit_per_hour():
    compactor = _make_compactor(hours=("12", "00", "05"), hourly=True)
    assert compactor._units_for(DATE) == [
        HourlyCompactionUnit(DATE, "00"),
        HourlyCompactionUnit(DATE, "05"),
        HourlyCompactionUnit(DATE, "12"),
    ]


def test_compact_flat_unit_full_flow():
    """A whole-date unit (no hour) compacts directly under the date partition, with no
    event_hour= subfolder anywhere in the source or destination paths.
    """
    source = _flat_source_blobs(3)
    staged = _flat_staging_blobs(1)
    compactor = _make_compactor(
        source_blobs=source, staging_blobs_sequence=[[], staged], hourly=False
    )

    compactor._compact(FLAT_UNIT, MagicMock())

    bucket = compactor.gcs_client.bucket.return_value
    deleted_names = [
        call.args[0] for call in bucket.blob.call_args_list if not call.args[0].endswith(".json")
    ]
    assert set(deleted_names) == {b.name for b in source}
    staged[0].delete.assert_called_once()
    bucket.copy_blob.assert_called_once()


# --- Compactor.run() ---


def test_run_processes_all_dates():
    compactor = _make_compactor()
    compactor.run([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)])

    prefixes = [call.kwargs["prefix"] for call in compactor.gcs_client.list_blobs.call_args_list]
    assert any("2024-01-01" in p for p in prefixes)
    assert any("2024-01-02" in p for p in prefixes)


def test_run_compacts_each_discovered_hour_independently():
    """Each discovered {prefix}hour= subpartition is compacted on its own, not merged
    together — this is what keeps partition depth uniform for the external table's
    hive partitioning.
    """
    hours = ("00", "05", "12")
    sources = {h: _source_blobs(3, hour=h) for h in hours}
    staged = {h: _staging_blobs(1, hour=h) for h in hours}
    dest_call_counts = dict.fromkeys(hours, 0)

    mock_gcs = MagicMock()

    def list_blobs(bucket, prefix, delimiter=None):
        if delimiter is not None:
            return _FakeIterator(prefixes={f"{prefix}event_hour={h}/" for h in hours})
        for h in hours:
            if f"event_hour={h}/" in prefix:
                if prefix.startswith(GCS_PATH.blob):
                    return iter(sources[h])
                # 1st call per hour is the pre-write existing-dest check (empty);
                # 2nd is the post-write listing (the freshly compacted output).
                dest_call_counts[h] += 1
                return iter([]) if dest_call_counts[h] == 1 else iter(staged[h])
        return iter([])

    mock_gcs.list_blobs.side_effect = list_blobs
    mock_gcs.bucket.return_value.blob.return_value.exists.return_value = False

    mock_conn = MagicMock()
    compactor = Compactor(
        gcs_client=mock_gcs,
        gcs_input_path=GCS_PATH,
        event_source="src",
        partition_prefix="event_",
        target_file_size_mb=512,
        hourly=True,
        conn_factory=MagicMock(return_value=mock_conn),
    )

    compactor.run([DATE])

    assert mock_conn.execute.call_count == len(hours)
    # Each unit gets and closes its own connection (via the shared MagicMock factory,
    # which returns the same mock instance every call, so this asserts once-per-unit).
    assert mock_conn.close.call_count == len(hours)


def test_run_parallel_gives_each_unit_its_own_independent_connection():
    """max_workers > 1 processes multiple units concurrently, each getting its own
    connection from conn_factory — never sharing one across units, since a
    retry-driven reconnect for one unit must never affect another running alongside it.
    """
    hours = ("00", "05", "12")
    sources = {h: _source_blobs(3, hour=h) for h in hours}
    staged = {h: _staging_blobs(1, hour=h) for h in hours}
    dest_call_counts = dict.fromkeys(hours, 0)

    mock_gcs = MagicMock()

    def list_blobs(bucket, prefix, delimiter=None):
        if delimiter is not None:
            return _FakeIterator(prefixes={f"{prefix}event_hour={h}/" for h in hours})
        for h in hours:
            if f"event_hour={h}/" in prefix:
                if prefix.startswith(GCS_PATH.blob):
                    return iter(sources[h])
                dest_call_counts[h] += 1
                return iter([]) if dest_call_counts[h] == 1 else iter(staged[h])
        return iter([])

    mock_gcs.list_blobs.side_effect = list_blobs
    mock_gcs.bucket.return_value.blob.return_value.exists.return_value = False

    created_connections = []

    def conn_factory(**kwargs):
        conn = MagicMock()
        created_connections.append(conn)
        return conn

    compactor = Compactor(
        gcs_client=mock_gcs,
        gcs_input_path=GCS_PATH,
        event_source="src",
        partition_prefix="event_",
        target_file_size_mb=512,
        hourly=True,
        max_workers=3,
        conn_factory=conn_factory,
    )

    compactor.run([DATE])

    assert len(created_connections) == len(hours)  # one independent connection per unit
    for conn in created_connections:
        conn.execute.assert_called_once()
        conn.close.assert_called_once()


# --- _copy_to_partition and _delete_blobs ---


def test_copy_to_partition_calls_gcs_copy():
    compactor = _make_compactor()
    blob = _make_blob(
        "path/messages/event_source=src/event_date=2024-01-01/event_hour=05/part_0.parquet"
    )

    compactor._copy_to_partition(UNIT, [blob], GCS_PATH)

    bucket = compactor.gcs_client.bucket.return_value
    expected_dest = (
        "path/messages/event_source=src/event_date=2024-01-01/event_hour=05/part_0.parquet"
    )
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
        gcs_input_path="gs://bucket/messages",
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
    mock_gcs.list_blobs.return_value = _FakeIterator()  # flat mode (default), no data found
    mock_gcs.bucket.return_value.blob.return_value.exists.return_value = False

    run(
        project="proj",
        gcs_input_path="gs://bucket/messages",
        event_source="src",
        start_date="2024-01-01",
        end_date="2024-01-02",
        gcs_client_factory=lambda project: mock_gcs,
        conn_factory=MagicMock,
    )

    mock_gcs.list_blobs.assert_called()


# --- run() HMAC / conn_factory selection ---


def test_run_raises_when_only_one_hmac_value_given():
    with pytest.raises(ValueError, match="hmac_key_id and hmac_secret"):
        run(
            project="proj",
            gcs_input_path="gs://bucket/messages",
            event_source="src",
            start_date="2024-01-01",
            end_date="2024-01-02",
            hmac_key_id="only-key-id",
            gcs_client_factory=lambda project: MagicMock(),
        )


def test_run_defaults_to_gcsfs_conn_factory_when_no_hmac():
    with patch("gfw.ops.pipelines.compact_parquet.main.Compactor") as mock_compactor_cls:
        run(
            project="proj",
            gcs_input_path="gs://bucket/messages",
            event_source="src",
            start_date="2024-01-01",
            end_date="2024-01-02",
            gcs_client_factory=lambda project: MagicMock(),
        )

    assert mock_compactor_cls.call_args.kwargs["conn_factory"] is _duckdb_conn


def test_run_builds_httpfs_conn_factory_when_hmac_given():
    with patch("gfw.ops.pipelines.compact_parquet.main.Compactor") as mock_compactor_cls:
        run(
            project="proj",
            gcs_input_path="gs://bucket/messages",
            event_source="src",
            start_date="2024-01-01",
            end_date="2024-01-02",
            hmac_key_id="my-key-id",
            hmac_secret="my-secret",
            gcs_client_factory=lambda project: MagicMock(),
        )

    conn_factory = mock_compactor_cls.call_args.kwargs["conn_factory"]
    assert isinstance(conn_factory, partial)
    assert conn_factory.func is _httpfs_duckdb_conn
    assert conn_factory.keywords == {"key_id": "my-key-id", "secret": "my-secret"}


def test_run_passes_max_workers_to_compactor():
    with patch("gfw.ops.pipelines.compact_parquet.main.Compactor") as mock_compactor_cls:
        run(
            project="proj",
            gcs_input_path="gs://bucket/messages",
            event_source="src",
            start_date="2024-01-01",
            end_date="2024-01-02",
            max_workers=5,
            gcs_client_factory=lambda project: MagicMock(),
        )

    assert mock_compactor_cls.call_args.kwargs["max_workers"] == 5
