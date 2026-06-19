from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class Job:
    id: int
    queue: str
    payload: Any
    status: str
    max_attempts: int
    attempts: int
    run_at: datetime
    created_at: datetime
    locked_at: Optional[datetime] = None


class AqueductClient:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def enqueue(self, queue: str, payload: Any, max_attempts: int = 3, run_at: Optional[datetime] = None) -> Job:
        raise NotImplementedError

    def cancel_job(self, job_id: int) -> bool:
        raise NotImplementedError

    def inspect_job(self, job_id: int) -> Optional[Job]:
        raise NotImplementedError
    

    
