"""GCS destination for hive-partitioned export."""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field

from cloudpathlib import GSPath
from google.cloud import bigquery, storage


_FORMAT_EXTENSION = {
    bigquery.DestinationFormat.PARQUET: "parquet",
    bigquery.DestinationFormat.AVRO: "avro",
    bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON: "json",
    bigquery.DestinationFormat.CSV: "csv",
}


@dataclass(frozen=True)
class HiveDestination:
    """GCS output path for a hive-partitioned export."""

    gcs_out: GSPath
    event_source: str
    partition_prefix: str = "event_"
    gcs_client: storage.Client = field(default_factory=storage.Client)
    destination_format: str = bigquery.DestinationFormat.PARQUET
    compression: str = bigquery.Compression.SNAPPY

    @property
    def extension(self) -> str:
        """File extension for the destination format."""
        return _FORMAT_EXTENSION[self.destination_format]

    @property
    def extract_job_config(self) -> bigquery.ExtractJobConfig:
        """Return a BQ extract job config for this destination's format."""
        return bigquery.ExtractJobConfig(
            destination_format=self.destination_format,
            compression=self.compression,
        )

    def partition_path(self, date: datetime.date) -> GSPath:
        """Return the GSPath for the hive partition directory of the given date."""
        p = self.partition_prefix
        return self.gcs_out / f"{p}source={self.event_source}" / f"{p}date={date.isoformat()}"

    def uri(self, date: datetime.date) -> str:
        """Return the full GCS destination URI for the given date."""
        return str(self.partition_path(date) / f"*.{self.extension}")

    def existing_dates(self, dates: list[datetime.date]) -> set[datetime.date]:
        """Return the subset of dates that already have files in GCS.

        Uses a single list_blobs call with delimiter="/" to retrieve one entry per
        date partition (not per file), then intersects with the requested dates.
        """
        p = self.partition_prefix
        source_path = self.gcs_out / f"{p}source={self.event_source}"
        source_prefix = f"{source_path.blob}/{p}date="
        blobs = self.gcs_client.list_blobs(
            source_path.bucket, prefix=source_prefix, delimiter="/"
        )
        existing = set()
        for page in blobs.pages:
            for prefix in page.prefixes:
                date_str = prefix.rstrip("/").split(f"{p}date=")[1]
                existing.add(datetime.date.fromisoformat(date_str))

        return existing & set(dates)
