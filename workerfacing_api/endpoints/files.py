import boto3
from fastapi import APIRouter, Depends, File, Request, status, UploadFile
from workerfacing_api.core.filesystem import filesystem_dep


router = APIRouter()
s3_client = boto3.client("s3")


@router.get("/files/{path:path}", status_code=status.HTTP_200_OK)
async def get_file(path: str, request: Request, url: bool = False, filesystem=Depends(filesystem_dep)) -> str:
    if url:
        return filesystem.get_file_url(path, request.url._url)
    else:
        return filesystem.get_file(path)


@router.post("/files/{path:path}", status_code=status.HTTP_201_CREATED)
async def post_file(path: str, file: UploadFile = File(...), filesystem=Depends(filesystem_dep)):
    return filesystem.post_file(file, path)
