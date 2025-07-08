import requests
from fastapi.encoders import jsonable_encoder

import workerfacing_api.settings as settings
from workerfacing_api.exceptions import JobDeletedException
from workerfacing_api.schemas.rds_models import JobStates


def update_job(
    job_id: int, job_status: JobStates, runtime_details: str | None = None
) -> None:
    body = {
        "job_id": job_id,
        "status": job_status.value,
        "runtime_details": runtime_details or "",
    }
    resp = requests.put(
        url=f"{settings.get_userfacing_api_url()}/_job_status",
        json=jsonable_encoder(body),
        headers={"x-api-key": settings.internal_api_key_secret},
    )
    if resp.status_code == 404:
        raise JobDeletedException(
            job_id, "it was probably deleted by the user"
        )
    resp.raise_for_status()
