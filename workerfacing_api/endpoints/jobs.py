import enum
import os
from fastapi import APIRouter, Depends, File, Query, status, Request, UploadFile

from workerfacing_api.core.queue import JobQueue
from workerfacing_api.dependencies import filesystem_dep, get_queue
from workerfacing_api.schemas.rds_models import JobStates
from workerfacing_api.schemas.queue_jobs import JobSpecs


router = APIRouter()


@router.get("/jobs", response_model=dict[int, JobSpecs], tags=["Jobs"])
async def get_jobs(
    hostname: str,
    cpu_cores: int,
    memory: int,
    environment: str,
    gpu_model: str | None = None,
    gpu_archi: str | None = None,
    gpu_mem: int | None = None,
    groups: list[str] | None = Query(None),
    limit: int = 1,
    older_than: int | None = None,
    queue: JobQueue = Depends(get_queue),
):
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


@router.get("/jobs/{job_id}/status", tags=["Jobs"])
async def get_job_status(job_id: int, queue: JobQueue = Depends(get_queue)):
    return queue.get_job(job_id).status


@router.put("/jobs/{job_id}/status", status_code=status.HTTP_200_OK, tags=["Jobs"])
async def put_job_status(
    job_id: int,
    status: JobStates,
    runtime_details: str | None = None,
    queue: JobQueue = Depends(get_queue),
):
    return queue.update_job_status(job_id, status, runtime_details)


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
async def post_file(
    job_id: int,
    type: UploadType,
    path: str,
    file: UploadFile = File(...),
    filesystem=Depends(filesystem_dep),
    queue: JobQueue = Depends(get_queue),
):
    job = queue.get_job(job_id)
    path = _upload_path(job, type, path)
    return filesystem.post_file(file, path)


@router.post(
    "/jobs/{job_id}/files/url", status_code=status.HTTP_201_CREATED, tags=["Files"]
)
async def post_file_url(
    job_id: int,
    type: UploadType,
    request: Request,
    base_path: str = "",
    filesystem=Depends(filesystem_dep),
    queue: JobQueue = Depends(get_queue),
):
    job = queue.get_job(job_id)
    path = _upload_path(job, type, base_path)
    return filesystem.post_file_url(path, request.url._url, "/url", "/upload")
