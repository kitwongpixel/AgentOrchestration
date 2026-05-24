"""Synchronous export filter validation and job queueing."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from threading import RLock
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


ALLOWED_STATUSES = {"pending", "running", "completed", "failed", "cancelled"}


class ExportError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class ExportFilterRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    date_from: Optional[str] = None
    date_to: Optional[str] = None
    workspace_id: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("workspace_id", "workspace"),
    )
    statuses: List[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("statuses", "status_filters"),
    )


@dataclass(frozen=True)
class ExportFilters:
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    workspace_id: Optional[str] = None
    statuses: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date_from": self.date_from,
            "date_to": self.date_to,
            "workspace_id": self.workspace_id,
            "statuses": list(self.statuses),
        }


@dataclass
class ExportJob:
    job_id: str
    created_at: datetime
    filters: ExportFilters
    status: str = "queued"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "filters": self.filters.to_dict(),
        }


class ExportJobService:
    def __init__(self):
        self._lock = RLock()
        self._jobs: Dict[str, ExportJob] = {}
        self._queue: List[str] = []

    def clear(self) -> None:
        with self._lock:
            self._jobs.clear()
            self._queue.clear()

    def queue_size(self) -> int:
        with self._lock:
            return len(self._queue)

    def list_jobs(self) -> List[ExportJob]:
        with self._lock:
            return [self._jobs[job_id] for job_id in self._queue]

    def get_job(self, job_id: str) -> Optional[ExportJob]:
        with self._lock:
            return self._jobs.get(job_id)

    def submit_export_job(
        self,
        *,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        workspace_id: Optional[str] = None,
        statuses: Optional[List[str]] = None,
    ) -> ExportJob:
        filters = self._normalize_filters(
            date_from=date_from,
            date_to=date_to,
            workspace_id=workspace_id,
            statuses=statuses or [],
        )
        job = ExportJob(
            job_id=str(uuid4()),
            created_at=datetime.now(timezone.utc),
            filters=filters,
        )
        with self._lock:
            self._jobs[job.job_id] = job
            self._queue.append(job.job_id)
        return job

    def _normalize_filters(
        self,
        *,
        date_from: Optional[str],
        date_to: Optional[str],
        workspace_id: Optional[str],
        statuses: List[str],
    ) -> ExportFilters:
        normalized_date_from = self._normalize_date(date_from, "date_from")
        normalized_date_to = self._normalize_date(date_to, "date_to")
        normalized_workspace_id = self._normalize_workspace_id(workspace_id)
        normalized_statuses = self._normalize_statuses(statuses)

        if (
            normalized_date_from
            and normalized_date_to
            and normalized_date_from > normalized_date_to
        ):
            raise ExportError(422, "date_from must be less than or equal to date_to")

        return ExportFilters(
            date_from=normalized_date_from,
            date_to=normalized_date_to,
            workspace_id=normalized_workspace_id,
            statuses=normalized_statuses,
        )

    def _normalize_date(self, value: Optional[str], field_name: str) -> Optional[str]:
        if value is None:
            return None
        try:
            normalized = date.fromisoformat(value).isoformat()
        except ValueError as exc:
            raise ExportError(422, f"{field_name} must be a valid ISO date (YYYY-MM-DD)") from exc
        return normalized

    def _normalize_workspace_id(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ExportError(422, "workspace_id must not be blank")
        return normalized

    def _normalize_statuses(self, statuses: List[str]) -> List[str]:
        normalized_statuses: List[str] = []
        invalid_statuses: List[str] = []

        for status in statuses:
            normalized = status.strip().lower()
            if not normalized:
                invalid_statuses.append(status)
                continue
            if normalized not in ALLOWED_STATUSES:
                invalid_statuses.append(status)
                continue
            normalized_statuses.append(normalized)

        if invalid_statuses:
            unique_invalid = ", ".join(dict.fromkeys(invalid_statuses))
            allowed = ", ".join(sorted(ALLOWED_STATUSES))
            raise ExportError(
                422,
                f"Unsupported export statuses: {unique_invalid}. Allowed: {allowed}",
            )

        deduped_statuses = list(dict.fromkeys(normalized_statuses))
        return deduped_statuses


export_jobs = ExportJobService()
