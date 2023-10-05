import boto3
from fastapi import APIRouter, Depends, Request, status

from workerfacing_api.dependencies import filesystem_dep


router = APIRouter()
s3_client = boto3.client("s3")


@router.get("/files/{path:path}/download", status_code=status.HTTP_200_OK)
async def download_file(path: str, filesystem=Depends(filesystem_dep)) -> str:
    return filesystem.get_file(path)


@router.get("/files/{path:path}/url", status_code=status.HTTP_200_OK)
async def url_file(
    path: str, request: Request, filesystem=Depends(filesystem_dep)
) -> str:
    return filesystem.get_file_url(path, request.url._url, "/files_url", "/files")
