import requests
import enum

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder

import workerfacing_api.settings as settings


class JobStates(enum.Enum):
    running = "running"
    finished = "finished"
    error = "error"


def update_job(job_id: int, job_status: JobStates) -> None:
    body = {
        "job_id": job_id,
        "status": job_status.value
    }
    resp = requests.post(url=f"{settings.userfacing_api_url}/updateJob", json=jsonable_encoder(body),
                         headers={"x-api-key": settings.internal_api_key_secret})
    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Error while updating job {job_id}. Traceback: \n{resp.text}."
        )
