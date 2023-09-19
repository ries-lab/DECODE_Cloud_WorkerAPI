from fastapi import APIRouter, HTTPException, UploadFile, status, Depends

import api.schemas as schemas
from api.dependencies import current_user_global_dep, filesystem_dep


router = APIRouter(dependencies=[Depends(current_user_global_dep)])


@router.get("/files/{base_path:path}", response_model=list[schemas.File])
def list_files(base_path: str | None = None, show_dirs: bool = True, recursive: bool = False, filesystem=Depends(filesystem_dep)):
    return filesystem.list_directory(base_path, dirs=show_dirs, recursive=recursive)


def upload_file(file_path: str, file: UploadFile, filesystem):
    filesystem.create_file(file_path, file.file)
    return filesystem.get_file_info(file_path)

@router.post("/files/config/{config_id}/{file_path:path}", response_model=schemas.File, status_code=status.HTTP_201_CREATED)
def upload_file_config(config_id: str, file_path: str, file: UploadFile, filesystem=Depends(filesystem_dep)):
    return upload_file(f"config/{config_id}/" + file_path, file, filesystem)

@router.post("/files/data/{data_id}/{file_path:path}", response_model=schemas.File, status_code=status.HTTP_201_CREATED)
def upload_file_data(data_id: str, file_path: str, file: UploadFile, filesystem=Depends(filesystem_dep)):
    return upload_file(f"data/{data_id}/" + file_path, file, filesystem)


@router.put("/files/{file_path:path}", response_model=schemas.File)
def rename_file(file_path: str, file: schemas.FileUpdate, filesystem=Depends(filesystem_dep)):
    filesystem.rename(file_path, file.path)
    return filesystem.get_file_info(file.path)


@router.delete("/files/{file_path:path}", status_code=status.HTTP_204_NO_CONTENT)
def delete_file(file_path: str, filesystem=Depends(filesystem_dep)):
    filesystem.delete(file_path)
    return {}


@router.get("/downloads/{file_path:path}", status_code=status.HTTP_200_OK)
def download_file(file_path: str, filesystem=Depends(filesystem_dep)):
    ret = filesystem.download(file_path)
    if not ret:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return ret
