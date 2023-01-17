from fastapi import APIRouter, UploadFile, status, Depends, Request

from api.crud import file as file_crud
import api.schemas as schemas
from api.dependencies import current_user_global_dep


router = APIRouter(dependencies=[Depends(current_user_global_dep)])


@router.get("/files", response_model=list[schemas.File])
def get_files(request: Request):
    return file_crud.list_user_files(request.state.current_user.username)


@router.get("/files/{file_path:path}", response_model=list[schemas.File])
def get_files_from_path(request: Request, file_path: str):
    return file_crud.list_user_files(request.state.current_user.username, file_path)


@router.post("/files/{file_path:path}", response_model=schemas.File)
def upload_file(request: Request, file_path: str, file: UploadFile):
    return file_crud.upload_file(request.state.current_user.username, file_path, file.file)


@router.put("/files/{file_path:path}", response_model=schemas.File)
def rename_file(request: Request, file_path: str, file: schemas.FileUpdate):
    return file_crud.rename_file(request.state.current_user.username, file_path, file.path)


@router.patch("/files/{file_path:path}", response_model=schemas.File)
def rename_file_patch(request: Request, file_path: str, file: schemas.FileUpdate):
    return file_crud.rename_file(request.state.current_user.username,  file_path, file.path)


@router.delete("/files/{file_path:path}", status_code=status.HTTP_204_NO_CONTENT)
def delete_file(request: Request, file_path: str):
    file_crud.delete_user_file(request.state.current_user.username, file_path)
    return {}
