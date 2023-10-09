import abc
import os
import re
import shutil
from fastapi import HTTPException, status
from fastapi.responses import FileResponse
from pathlib import Path


class FileSystem(abc.ABC):
    def __init__(self):
        pass

    def get_file(self, path: str):
        raise NotImplementedError()

    def get_file_url(
        self, path: str, request_url: str, url_endpoint: str, files_endpoint: str
    ):
        raise NotImplementedError()

    def post_file(self, file, path: str):
        raise NotImplementedError
    
    def post_file_url(self, path: str, request_url: str, url_endpoint: str, files_endpoint: str):
        raise NotImplementedError()


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

    def get_file_url(
        self, path: str, request_url: str, url_endpoint: str, files_endpoint: str
    ):
        if not os.path.exists(path):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return re.sub(url_endpoint, files_endpoint, request_url, 1)

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
    
    def post_file_url(self, path: str, request_url: str, url_endpoint: str, files_endpoint: str):
        if not Path(self.base_post_path) in Path(path).parents:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Path is not in base directory",
            )
        return {"url": re.sub(url_endpoint, files_endpoint, request_url, 1), "fields": {}}


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
        if not path.startswith("s3://"):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        bucket, _, path = path[5:].partition("/")
        if not bucket == self.bucket:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return bucket, path

    def get_file_url(
        self, path: str, request_url: str, url_endpoint: str, files_endpoint: str
    ):
        bucket, path = self._get_bucket_path(path)

        response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
        if not "Contents" in response:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

        return self.s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": path},
            ExpiresIn=60 * 10,
        )

    def post_file(self, file, path: str):
        bucket, path = self._get_bucket_path(path)
        self.s3_client.upload_fileobj(file.file, bucket, path)
    
    def post_file_url(self, path: str, request_url: str, url_endpoint: str, files_endpoint: str):
        bucket, path = self._get_bucket_path(path)
        if path[-1] != "/":
            path = path + "/"
        return self.s3_client.generate_presigned_post(
            Bucket=bucket,
            Key=path + "${filename}",
            Fields=None,
            Conditions=[["starts-with", "$key", path]],  # can be used for multiple uploads to folder
            ExpiresIn=60 * 10,
        )
