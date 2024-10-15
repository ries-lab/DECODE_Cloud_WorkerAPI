import re

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import FileResponse

from workerfacing_api.dependencies import filesystem_dep
from workerfacing_api.schemas.files import FileHTTPRequest

router = APIRouter()


@router.get(
    "/files/{file_id:path}/download",
    status_code=status.HTTP_200_OK,
    response_class=FileResponse,
)
async def download_file(file_id: str, filesystem=Depends(filesystem_dep)) -> str:
    return filesystem.get_file(path=file_id)


@router.get(
    "/files/{file_id:path}/url",
    status_code=status.HTTP_200_OK,
    response_model=FileHTTPRequest,
)
async def get_download_presigned_url(
    file_id: str, request: Request, filesystem=Depends(filesystem_dep)
) -> str:
    return filesystem.get_file_url(
        path=file_id,
        request=request,
        url_endpoint=re.escape("/url") + "$",
        files_endpoint="/download",
    )
