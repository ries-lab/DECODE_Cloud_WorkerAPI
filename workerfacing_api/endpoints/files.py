import boto3
import enum
import os
from fastapi import APIRouter, Depends, File, Request, status, UploadFile

from workerfacing_api.core.queue import JobQueue
from workerfacing_api.dependencies import filesystem_dep, get_queue


router = APIRouter()
s3_client = boto3.client("s3")


@router.get("/files/{path:path}", status_code=status.HTTP_200_OK)
async def get_file(path: str, filesystem=Depends(filesystem_dep)) -> str:
    return filesystem.get_file(path)


@router.get("/files_url/{path:path}", status_code=status.HTTP_200_OK)
async def get_file_url(path: str, request: Request, filesystem=Depends(filesystem_dep)) -> str:
    return filesystem.get_file_url(path, request.url._url, "/files_url", "/files")


class UploadType(enum.Enum):
    output = "output"
    log = "log"
    artifact = "artifact"


@router.post("/files/{job_id}/{path:path}", status_code=status.HTTP_201_CREATED)
async def post_file(
    job_id: int,
    type: UploadType,
    path: str,
    file: UploadFile = File(...),
    filesystem=Depends(filesystem_dep),
    queue: JobQueue = Depends(get_queue),
):
    job = queue.get_job(job_id)
    path = os.path.join(job.paths_upload[type.value], path)  # not pathlib.Path since it does s3://x => s3:/x
    return filesystem.post_file(file, path)
