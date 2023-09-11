import abc
import boto3
import os
import shutil
from fastapi import HTTPException, status
from fastapi.responses import FileResponse
from pathlib import Path
from tempfile import NamedTemporaryFile

import workerfacing_api.settings as settings



class FileSystem(abc.ABC):
    def __init__(self):
        pass

    def get_file(self, path: str):
        raise NotImplementedError()

    def get_file_url(self, path: str, request_url: str):
        raise NotImplementedError()

    def post_file(self, path: str):
        raise NotImplementedError


class LocalFilesystem(FileSystem):
    def __init__(self, base_get_path, base_post_path):
        self.base_get_path = base_get_path
        self.base_post_path = base_post_path

    def get_file(self, path: str):
        if not Path(self.base_get_path) in Path(path).parents:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Path is not in base directory",
            )
        if not os.path.exists(path):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return FileResponse(path)

    def get_file_url(self, path: str, request_url: str):
        if not os.path.exists(path):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return f"{request_url}?url=1"

    def post_file(self, file, path: str):
        if not Path(self.base_post_path) in Path(path).parents:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Path is not in base directory",
            )
        try:
            os.makedirs(Path(path).parent)
            with open(path, "wb") as f:
                shutil.copyfileobj(file.file, f)
        finally:
            file.file.close()


class S3Filesystem(FileSystem):
    def __init__(self, s3_client, bucket):
        self.s3_client = s3_client
        self.bucket = bucket

    def get_file(self, path: str):
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="Please get a pre-signed url instead.",
        )

    def _get_bucket_path(self, path):
        assert path.startswith("s3://")
        bucket, _, path = path[5:].partition("/")
        assert bucket == self.bucket
        return bucket, path

    def get_file_url(self, path: str, request_url: str):
        try:
            bucket, path = self._get_bucket_path(path)
            resp = self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": path},
                ExpiresIn=60*5,
            )
            return resp
        except:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Wrong s3 url",
            )

    def post_file(self, file, path: str):
        bucket, path = self._get_bucket_path(path)
        self.s3_client.upload_fileobj(file.file, bucket, path)


async def filesystem_dep():
    if settings.filesystem == 's3':
        s3_client = boto3.client('s3')
        s3_bucket = settings.s3_bucket
        return S3Filesystem(s3_client, s3_bucket)
    elif settings.filesystem == 'local':
        return LocalFilesystem(settings.user_data_root_path, settings.models_root_path)
    else:
        raise ValueError('Invalid filesystem setting')
