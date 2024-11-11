import re

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import FileResponse

from workerfacing_api.core.filesystem import FileSystem
from workerfacing_api.dependencies import filesystem_dep
from workerfacing_api.schemas.files import FileHTTPRequest

router = APIRouter()


@router.get(
    "/files/{file_id:path}/download",
    response_class=FileResponse,
    description="Download a file from the filesystem.",
)
async def download_file(
    file_id: str, filesystem: FileSystem = Depends(filesystem_dep)
) -> FileResponse:
    try:
        return filesystem.get_file(path=file_id)
    except FileNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    except PermissionError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))


@router.get(
    "/files/{file_id:path}/url",
    response_model=FileHTTPRequest,
    description="Get request parameters to download a file from the filesystem.",
)
async def get_download_presigned_url(
    file_id: str, request: Request, filesystem: FileSystem = Depends(filesystem_dep)
) -> FileHTTPRequest:
    try:
        return filesystem.get_file_url(
            path=file_id,
            request=request,
            url_endpoint=re.escape("/url") + "$",
            files_endpoint="/download",
        )
    except FileNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    except PermissionError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
