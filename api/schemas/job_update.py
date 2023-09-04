from pydantic import BaseModel

from api.models import JobStates


class JobUpdate(BaseModel):
    job_id: int
    status: JobStates
