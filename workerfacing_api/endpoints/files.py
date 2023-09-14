import boto3
import os
from fastapi import APIRouter, Depends, File, Request, status, UploadFile

from workerfacing_api.core.filesystem import filesystem_dep
from workerfacing_api.core.queue import JobQueue
from workerfacing_api.queue import get_queue


router = APIRouter()
s3_client = boto3.client("s3")


@router.get("/files/{path:path}", status_code=status.HTTP_200_OK)
async def get_file(path: str, request: Request, url: bool = True, filesystem=Depends(filesystem_dep)) -> str:
    if url:
        return filesystem.get_file_url(path, request.url._url)
    else:
        return filesystem.get_file(path)


@router.post("/files/{job_id}", status_code=status.HTTP_201_CREATED)
async def post_file(job_id: int, file: UploadFile = File(...), filesystem=Depends(filesystem_dep), queue: JobQueue = Depends(get_queue)):
    job = queue.get_job(job_id)
    path = os.path.join(job.path_upload, file.filename)  # not pathlib.Path since it does s3://x => s3:/x
    return filesystem.post_file(file, path)
