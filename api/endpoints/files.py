from fastapi import APIRouter, HTTPException, UploadFile, status, Depends
from sqlalchemy.orm import Session

import api.schemas as schemas
from api import database, crud
from api.dependencies import current_user_global_dep, filesystem_dep


router = APIRouter(dependencies=[Depends(current_user_global_dep)])


@router.get("/files", response_model=list[schemas.File])
def get_files(recursive: bool = False, filesystem=Depends(filesystem_dep)):
    return filesystem.list_directory('', recursive=recursive)


@router.get("/files/{file_path:path}", response_model=list[schemas.File])
def get_files_from_path(file_path: str, recursive: bool = False, filesystem=Depends(filesystem_dep)):
    return filesystem.list_directory(file_path, recursive=recursive)


def upload_file(file_path: str, file: UploadFile, filesystem):
    filesystem.create_file(file_path, file.file)
    return filesystem.get_file_info(file_path)

@router.post("/files/config/{config_id}/{file_path:path}", response_model=schemas.File, status_code=status.HTTP_201_CREATED)
def upload_file_config(config_id: str, file_path: str, file: UploadFile, filesystem=Depends(filesystem_dep)):
    return upload_file(f"config/{config_id}/" + file_path, file, filesystem)

@router.post("/files/data/{data_id}/{file_path:path}", response_model=schemas.File, status_code=status.HTTP_201_CREATED)
def upload_file_data(data_id: str, file_path: str, file: UploadFile, filesystem=Depends(filesystem_dep)):
    return upload_file(f"data/{data_id}/" + file_path, file, filesystem)


def rename_file(file_path: str, file: schemas.FileUpdate, filesystem):
    filesystem.rename(file_path, file.path)
    return filesystem.get_file_info(file.path)

@router.put("/files/config/{config_id}/{file_path:path}", response_model=schemas.File)
def rename_file_config(config_id: str, file_path: str, file: schemas.FileUpdate, filesystem=Depends(filesystem_dep)):
    return rename_file(f"config/{config_id}/" + file_path, "config/" + file, filesystem)

@router.patch("/files/config/{config_id}/{file_path:path}", response_model=schemas.File)
def rename_file_config_patch(config_id: str, file_path: str, file: schemas.FileUpdate, filesystem=Depends(filesystem_dep)):
    return rename_file(f"config/{config_id}/" + file_path, "config/" + file, filesystem)

@router.put("/files/data/{data_id}/{file_path:path}", response_model=schemas.File)
def rename_file_data(data_id: str, file_path: str, file: schemas.FileUpdate, filesystem=Depends(filesystem_dep)):
    return rename_file(f"data/{data_id}/" + file_path, "data/" + file, filesystem)

@router.patch("/files/data/{file_path:path}", response_model=schemas.File)
def rename_file_data_patch(data_id: str, file_path: str, file: schemas.FileUpdate, filesystem=Depends(filesystem_dep)):
    return rename_file(f"data/{data_id}/" + file_path, "data/" + file, filesystem)


@router.delete("/files/{file_path:path}", status_code=status.HTTP_204_NO_CONTENT)
def delete_file(file_path: str, filesystem=Depends(filesystem_dep)):
    filesystem.delete(file_path)
    return {}
