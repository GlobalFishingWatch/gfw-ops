"""Example: consolidate sharded AIS normalized tables into a single daily-partitioned table."""
from __future__ import annotations

from pathlib import Path

from gfw.ops.pipelines import sharded_to_partitioned as stp


def main() -> None:
    stp.run(
        bq_in_sharded=[
            "world-fishing-827.pipe_ais_sources_v20220628.normalized_orbcomm",
            "world-fishing-827.pipe_ais_sources_v20220628.normalized_spire",
            "world-fishing-827.pipe_ais_sources_v20220628.pipe_nmea_normalized",
            "world-fishing-827.pipe_ais_sources_v20220628.pipe_nmea_marinetraffic_normalized",
        ],
        bq_out_partitioned="world-fishing-827.pipe_ais_sources_v20220628.normalized_consolidated",
        project="world-fishing-827",
        schema_file=str(Path(__file__).parent / "assets/normalize-schema.json"),
        start_date="2012-01",
        end_date="2026-04",
        partition_field="timestamp",
        dry_run=True,
        overwrite=False,
        limit=1,
    )


if __name__ == "__main__":
    main()
