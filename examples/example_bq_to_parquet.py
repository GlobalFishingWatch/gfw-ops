"""Example: export a date range from a BigQuery table to hive-partitioned Parquet on GCS."""
from __future__ import annotations

from pathlib import Path

from gfw.ops.pipelines import bq_to_parquet as pipeline


def main() -> None:
    pipeline.run(
        project="world-fishing-827",
        bq_in="world-fishing-827.pipe_ais_sources_v20220628.normalized_consolidated",
        gcs_out="gs://gfw-int-ais-datalake-vessel-transmissions-v1/normalized_consolidated",
        schema_file=str(Path(__file__).parent / "assets/normalize-schema.json"),
        date_range=("2024-01-01", "2024-01-31"),
        timestamp_field="timestamp",
        partition_time_granularity="hour",
        partition_prefix="event_",
        gcs_window_size=3600,
        gcs_num_shards=6,
        runner="DirectRunner",
        dry_run=True,
    )


if __name__ == "__main__":
    main()
