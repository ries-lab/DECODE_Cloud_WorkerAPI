from fastapi import APIRouter, UploadFile, status

from crud import file as file_crud
import schemas
from settings import user_id


router = APIRouter()


@router.get("/files", response_model=list[schemas.File])
def get_files():
    return file_crud.list_user_files(user_id)


@router.get("/files/{file_path:path}", response_model=list[schemas.File])
def get_files_from_path(file_path: str):
    return file_crud.list_user_files(user_id, file_path)


@router.post("/files/{file_path:path}", response_model=schemas.File)
def upload_file(file_path: str, file: UploadFile):
    return file_crud.upload_file(user_id, file_path, file.file)


@router.put("/files/{file_path:path}", response_model=schemas.File)
def rename_file(file_path: str, file: schemas.FileUpdate):
    return file_crud.rename_file(user_id, file_path, file.name)


@router.patch("/files/{file_path:path}", response_model=schemas.File)
def rename_file_patch(file_path: str, file: schemas.FileUpdate):
    return file_crud.rename_file(user_id, file_path, file.name)


@router.delete("/files/{file_path:path}", status_code=status.HTTP_204_NO_CONTENT)
def delete_file(file_path: str):
    file_crud.delete_user_file(user_id, file_path)
    return {}
