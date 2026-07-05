"""BQ extract job tracking types."""
from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from enum import auto, StrEnum

from google.api_core.exceptions import NotFound
from google.cloud import bigquery


logger = logging.getLogger(__name__)


class ExportStatus(StrEnum):
    """Outcome of a single BQ extract job."""

    SUCCESS = auto()
    NOT_FOUND = auto()
    FAILED = auto()


@dataclass
class ExportJob:
    """A submitted BQ extract job paired with the date it covers."""

    date: datetime.date
    job: bigquery.ExtractJob
    status: ExportStatus | None = field(default=None, init=False, repr=False)

    def wait(self) -> ExportJob:
        """Block until the extract job completes and set status. Returns self."""
        try:
            self.job.result()
            logger.info(f"Completed {self.job.job_id}")
            self.status = ExportStatus.SUCCESS
        except NotFound:
            logger.warning(f"Skipped {self.date}: not found")
            self.status = ExportStatus.NOT_FOUND
        except Exception:
            logger.exception(f"Failed {self.job.job_id} for {self.date}")
            self.status = ExportStatus.FAILED

        return self


@dataclass
class ExportJobResults:
    """Collection of completed export jobs."""

    jobs: list[ExportJob]

    @property
    def failed(self) -> list[ExportJob]:
        """Jobs that failed with an unexpected error."""
        return self.filter(ExportStatus.FAILED)

    @property
    def succeeded(self) -> list[ExportJob]:
        """Jobs that completed successfully."""
        return self.filter(ExportStatus.SUCCESS)

    @property
    def skipped(self) -> list[ExportJob]:
        """Jobs skipped because the source table or shard was not found."""
        return self.filter(ExportStatus.NOT_FOUND)

    def filter(self, status: ExportStatus) -> list[ExportJob]:
        """Return jobs matching the given status."""
        return [job for job in self.jobs if job.status == status]
