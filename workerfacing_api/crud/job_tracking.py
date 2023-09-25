import requests
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder

import workerfacing_api.settings as settings
from workerfacing_api.schemas.rds_models import JobStates


def update_job(job_id: int, job_status: JobStates) -> None:
    body = {"job_id": job_id, "status": job_status.value}
    resp = requests.put(
        url=f"{settings.get_userfacing_api_url()}/_job_status",
        json=jsonable_encoder(body),
        headers={"x-api-key": settings.internal_api_key_secret},
    )
    if not str(resp.status_code).startswith("2"):
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Error while updating job {job_id}. Traceback: \n{resp.text}.",
        )
