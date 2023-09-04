import api.settings as settings
import requests
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder


def get_enqueueing_function() -> callable:
    def enqueue(queue_item: dict) -> None:
        resp = requests.post(url=f"{settings.workerfacing_api_url}/jobs", json=jsonable_encoder(queue_item), headers={"x-api-key": settings.internal_api_key_secret})
        if resp.status_code != 200:
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"Error while enqueuing job {queue_item['job_id']}. Traceback: \n{resp.text}."
            )
    return enqueue
