import enum
import os

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi import (
    status as httpstatus,
)

from workerfacing_api.core.filesystem import FileSystem
from workerfacing_api.core.queue import RDSJobQueue
from workerfacing_api.dependencies import filesystem_dep, queue_dep
from workerfacing_api.schemas.files import FileHTTPRequest
from workerfacing_api.schemas.queue_jobs import (
    EnvironmentTypes,
    JobFilter,
    JobSpecs,
)
from workerfacing_api.schemas.rds_models import JobStates, QueuedJob

router = APIRouter()


@router.get(
    "/jobs",
    response_model=dict[int, JobSpecs],
    tags=["Jobs"],
    description="Pull jobs from the queue",
)
async def get_jobs(
    request: Request,
    memory: int,
    cpu_cores: int = 1,
    gpu_mem: int = 0,
    gpu_model: str | None = None,
    gpu_archi: str | None = None,
    groups: list[str] | None = Query(None),
    limit: int = 1,
    older_than: int = 0,
    queue: RDSJobQueue = Depends(queue_dep),
) -> dict[int, JobSpecs]:
    hostname = request.state.current_user.username
    environment = (
        EnvironmentTypes.cloud
        if "cloud" in request.state.current_user.cognito_groups
        else EnvironmentTypes.local
    )

    jobs = {}
    for _ in range(limit):
        try:
            res = queue.dequeue(
                hostname=hostname,
                filter=JobFilter(
                    cpu_cores=cpu_cores,
                    memory=memory,
                    environment=environment,
                    gpu_model=gpu_model,
                    gpu_archi=gpu_archi,
                    gpu_mem=gpu_mem,
                    groups=groups,
                    older_than=older_than,
                ),
            )
        except ValueError as e:
            raise HTTPException(
                status_code=httpstatus.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
            )
        if res:
            jobs.update({res[0]: res[1]})
        else:
            break
    return jobs


@router.get(
    "/jobs/{job_id}/status",
    tags=["Jobs"],
    response_model=JobStates,
    description="Get the status of a job",
)
async def get_job_status(
    job_id: int, queue: RDSJobQueue = Depends(queue_dep)
) -> JobStates:
    try:
        return queue.get_job(job_id).status  # type: ignore
    except RuntimeError:
        raise HTTPException(status_code=httpstatus.HTTP_404_NOT_FOUND)


@router.put(
    "/jobs/{job_id}/status",
    tags=["Jobs"],
    status_code=httpstatus.HTTP_204_NO_CONTENT,
    description="Update the status of a job (or ping for keep-alive)",
)
async def put_job_status(
    request: Request,
    job_id: int,
    status: JobStates,
    runtime_details: str | None = None,
    queue: RDSJobQueue = Depends(queue_dep),
) -> None:
    hostname = request.state.current_user.username
    try:
        queue.update_job_status(job_id, status, runtime_details, hostname=hostname)
    except ValueError:
        # acts as a "cancel job" signal to worker
        raise HTTPException(status_code=httpstatus.HTTP_404_NOT_FOUND)


class UploadType(enum.Enum):
    output = "output"
    log = "log"
    artifact = "artifact"


def _upload_path(job: QueuedJob, type: UploadType, path: str) -> str:
    return os.path.join(
        job.paths_upload[type.value], path
    )  # not pathlib.Path since it does s3://x => s3:/x


@router.post(
    "/jobs/{job_id}/files/upload",
    status_code=httpstatus.HTTP_201_CREATED,
    tags=["Files"],
    description="Upload a file to the job's output, log or artifact directory",
)
async def upload_file(
    request: Request,
    job_id: int,
    type: UploadType,
    base_path: str,
    file: UploadFile = File(...),
    filesystem: FileSystem = Depends(filesystem_dep),
    queue: RDSJobQueue = Depends(queue_dep),
) -> None:
    try:
        job = queue.get_job(job_id, hostname=request.state.current_user.username)
    except ValueError:
        raise HTTPException(status_code=httpstatus.HTTP_404_NOT_FOUND)
    path = _upload_path(job, type, base_path)
    try:
        return filesystem.post_file(file, path)
    except PermissionError as e:
        raise HTTPException(status_code=httpstatus.HTTP_403_FORBIDDEN, detail=str(e))


@router.post(
    "/jobs/{job_id}/files/url",
    status_code=httpstatus.HTTP_201_CREATED,
    response_model=FileHTTPRequest,
    tags=["Files"],
    description="Get a presigned URL to upload a file to the job's output, log or artifact directory",
)
async def get_upload_presigned_url(
    request: Request,
    job_id: int,
    type: UploadType,
    base_path: str = "",
    filesystem: FileSystem = Depends(filesystem_dep),
    queue: RDSJobQueue = Depends(queue_dep),
) -> FileHTTPRequest:
    try:
        job = queue.get_job(job_id, hostname=request.state.current_user.username)
    except ValueError:
        raise HTTPException(status_code=httpstatus.HTTP_404_NOT_FOUND)
    path = _upload_path(job, type, base_path)
    try:
        return filesystem.post_file_url(path, request, "/url", "/upload")
    except PermissionError as e:
        raise HTTPException(status_code=httpstatus.HTTP_403_FORBIDDEN, detail=str(e))
