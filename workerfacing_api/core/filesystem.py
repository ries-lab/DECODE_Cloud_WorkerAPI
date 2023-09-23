import abc
import os
import shutil
from fastapi import HTTPException, status
from fastapi.responses import FileResponse
from pathlib import Path


class FileSystem(abc.ABC):
    def __init__(self):
        pass

    def get_file(self, path: str):
        raise NotImplementedError()

    def get_file_url(self, path: str, request_url: str, url_endpoint: str, files_endpoint: str):
        raise NotImplementedError()

    def post_file(self, file, path: str):
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

    def get_file_url(self, path: str, request_url: str, url_endpoint: str, files_endpoint: str):
        if not os.path.exists(path):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return request_url.replace(url_endpoint, files_endpoint, 1)

    def post_file(self, file, path: str):
        if not Path(self.base_post_path) in Path(path).parents:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Path is not in base directory",
            )
        try:
            os.makedirs(Path(path).parent, exist_ok=True)
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

    def get_file_url(self, path: str, request_url: str, url_endpoint: str, files_endpoint: str):
        bucket, path = self._get_bucket_path(path)
        resp = self.s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": path},
            ExpiresIn=60*10,
        )
        if not resp:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return resp

    def post_file(self, file, path: str):
        bucket, path = self._get_bucket_path(path)
        self.s3_client.upload_fileobj(file.file, bucket, path)
