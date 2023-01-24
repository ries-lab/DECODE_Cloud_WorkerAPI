from fastapi import APIRouter, UploadFile, status, Depends

import api.schemas as schemas
from api.dependencies import current_user_global_dep, filesystem_dep


router = APIRouter(dependencies=[Depends(current_user_global_dep)])


@router.get("/files", response_model=list[schemas.File])
def get_files(filesystem=Depends(filesystem_dep)):
    return filesystem.list_directory('')


@router.get("/files/{file_path:path}", response_model=list[schemas.File])
def get_files_from_path(file_path: str, filesystem=Depends(filesystem_dep)):
    return filesystem.list_directory(file_path)


@router.post("/files/{file_path:path}", response_model=schemas.File, status_code=status.HTTP_201_CREATED)
def upload_file(file_path: str, file: UploadFile, filesystem=Depends(filesystem_dep)):
    filesystem.create_file(file_path, file.file)
    return filesystem.get_file_info(file_path)


@router.put("/files/{file_path:path}", response_model=schemas.File)
def rename_file(file_path: str, file: schemas.FileUpdate, filesystem=Depends(filesystem_dep)):
    filesystem.rename(file_path, file.path)
    return filesystem.get_file_info(file.path)


@router.patch("/files/{file_path:path}", response_model=schemas.File)
def rename_file_patch(file_path: str, file: schemas.FileUpdate, filesystem=Depends(filesystem_dep)):
    filesystem.rename(file_path, file.path)
    return filesystem.get_file_info(file.path)


@router.delete("/files/{file_path:path}", status_code=status.HTTP_204_NO_CONTENT)
def delete_file(file_path: str, filesystem=Depends(filesystem_dep)):
    filesystem.delete(file_path)
    return {}
