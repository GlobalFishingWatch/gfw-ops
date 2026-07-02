"""Example: export a date range from a BigQuery table to hive-partitioned Parquet on GCS."""
from __future__ import annotations

from gfw.ops.pipelines import bq_to_parquet as pipeline


def main() -> None:
    pipeline.run(
        project="world-fishing-827",
        bq_in="world-fishing-827.pipe_ais_sources_v20201001.pipe-nmea-parsed",
        gcs_out="gs://gfw-int-ais-datalake-vessel-transmissions-v1/pipe-nmea-parsed",
        event_source="wf827-pipe-nmea-parsed",
        start_date="2024-01-01",
        end_date="2024-02-01",
        partition_prefix="event_",
        dry_run=True,
    )


if __name__ == "__main__":
    main()
