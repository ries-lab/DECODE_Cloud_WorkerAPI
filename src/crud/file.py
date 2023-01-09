from fastapi import HTTPException, status
import os

from core.filesystem import LocalFilesystem
from core.user import get_user_data_root


def list_user_files(user_id: str, path: str = ''):
    filesystem = LocalFilesystem(get_user_data_root(user_id))
    return filesystem.list_directory(path)


def upload_file(user_id: str, path: str, file):
    filesystem = LocalFilesystem(get_user_data_root(user_id))
    filesystem.create_file(path, file)
    return filesystem.get_file_info(path)


def rename_file(user_id: str, path: str, new_name: str):
    if os.path.split(new_name)[0] != '':
        raise HTTPException(status.HTTP_400_BAD_REQUEST, 'New name must not contain a path')
    filesystem = LocalFilesystem(get_user_data_root(user_id))
    new_path = filesystem.rename(path, new_name)
    return filesystem.get_file_info(new_path)


def delete_user_file(user_id: str, path: str):
    filesystem = LocalFilesystem(get_user_data_root(user_id))
    return filesystem.delete(path)
