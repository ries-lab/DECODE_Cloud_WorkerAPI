import enum
import os
import re

from fastapi import APIRouter, Depends, File, Query, Request, UploadFile, status

from workerfacing_api.core.queue import JobQueue
from workerfacing_api.dependencies import filesystem_dep, get_queue
from workerfacing_api.schemas.files import FileHTTPRequest
from workerfacing_api.schemas.queue_jobs import JobSpecs
from workerfacing_api.schemas.rds_models import JobStates

router = APIRouter()


@router.get("/jobs", response_model=dict[int, JobSpecs], tags=["Jobs"])
async def get_jobs(
    request: Request,
    cpu_cores: int,
    memory: int,
    gpu_model: str | None = None,
    gpu_archi: str | None = None,
    gpu_mem: int | None = None,
    groups: list[str] | None = Query(None),
    limit: int = 1,
    older_than: int | None = None,
    queue: JobQueue = Depends(get_queue),
):
    hostname = request.state.current_user.username
    environment = (
        "cloud" if "cloud" in request.state.current_user.cognito_groups else "local"
    )

    jobs = {}
    for _ in range(limit):
        job = queue.dequeue(
            hostname=hostname,
            cpu_cores=cpu_cores,
            memory=memory,
            environment=environment,
            gpu_model=gpu_model,
            gpu_archi=gpu_archi,
            gpu_mem=gpu_mem or 0,
            groups=groups,
            older_than=older_than or 0,
        )
        if job:
            job_id = job.pop("job_id")
            jobs.update({job_id: JobSpecs(**job)})
        else:
            break
    return jobs


@router.get("/jobs/{job_id}/status", tags=["Jobs"], response_model=JobStates)
async def get_job_status(job_id: int, queue: JobQueue = Depends(get_queue)):
    return queue.get_job(job_id).status


@router.put("/jobs/{job_id}/status", tags=["Jobs"])
async def put_job_status(
    request: Request,
    job_id: int,
    status: JobStates,
    runtime_details: str | None = None,
    queue: JobQueue = Depends(get_queue),
):
    hostname = request.state.current_user.username
    return queue.update_job_status(job_id, status, runtime_details, hostname=hostname)


class UploadType(enum.Enum):
    output = "output"
    log = "log"
    artifact = "artifact"


def _upload_path(job, type, path):
    return os.path.join(
        job.paths_upload[type.value], path
    )  # not pathlib.Path since it does s3://x => s3:/x


@router.post(
    "/jobs/{job_id}/files/upload", status_code=status.HTTP_201_CREATED, tags=["Files"]
)
async def upload_file(
    request: Request,
    job_id: int,
    type: UploadType,
    path: str,
    file: UploadFile = File(...),
    filesystem=Depends(filesystem_dep),
    queue: JobQueue = Depends(get_queue),
):
    job = queue.get_job(job_id, hostname=request.state.current_user.username)
    path = _upload_path(job, type, path)
    return filesystem.post_file(file, path)


@router.post(
    "/jobs/{job_id}/files/url",
    status_code=status.HTTP_201_CREATED,
    response_model=FileHTTPRequest,
    tags=["Files"],
)
async def get_upload_presigned_url(
    request: Request,
    job_id: int,
    type: UploadType,
    base_path: str = "",
    filesystem=Depends(filesystem_dep),
    queue: JobQueue = Depends(get_queue),
):
    job = queue.get_job(job_id, hostname=request.state.current_user.username)
    path = _upload_path(job, type, base_path)
    return filesystem.post_file_url(path, request, re.escape("/url") + "$", "/upload")
