import abc
import os
import re
import shutil
from pathlib import Path

from fastapi import Request, UploadFile
from fastapi.responses import FileResponse
from mypy_boto3_s3 import S3Client

from workerfacing_api.schemas.files import FileHTTPRequest


class FileSystem(abc.ABC):
    def get_file(self, path: str) -> FileResponse:
        """Donwload a file from the filesystem."""
        raise NotImplementedError()

    def get_file_url(
        self, path: str, request: Request, url_endpoint: str, files_endpoint: str
    ) -> FileHTTPRequest:
        """Get a url + parameters to request a file from the filesystem."""
        raise NotImplementedError()

    def post_file(self, file: UploadFile, path: str) -> None:
        """Upload a file to the filesystem."""
        raise NotImplementedError

    def post_file_url(
        self, path: str, request: Request, url_endpoint: str, files_endpoint: str
    ) -> FileHTTPRequest:
        """Get a url + parameters to upload a file to the filesystem."""
        raise NotImplementedError()


class LocalFilesystem(FileSystem):
    """Filesystem on local disk."""

    def __init__(self, base_get_path: str, base_post_path: str):
        self.base_get_path = base_get_path
        self.base_post_path = base_post_path

    def get_file(self, path: str) -> FileResponse:
        if Path(self.base_get_path) not in Path(path).parents:
            raise PermissionError("Path is not in base directory")
        if not os.path.exists(path):
            raise FileNotFoundError()
        return FileResponse(path)

    def get_file_url(
        self, path: str, request: Request, url_endpoint: str, files_endpoint: str
    ) -> FileHTTPRequest:
        if Path(self.base_get_path) not in Path(path).parents:
            raise PermissionError("Path is not in base directory")
        if not os.path.exists(path):
            raise FileNotFoundError()
        return FileHTTPRequest(
            url=re.sub(url_endpoint, files_endpoint, request.url._url),
            method="get",
            headers=(
                {"authorization": request.headers["authorization"]}
                if "authorization" in request.headers
                else {}
            ),
        )

    def post_file(self, file: UploadFile, path: str) -> None:
        if Path(self.base_post_path) not in Path(path).parents:
            raise PermissionError("Path is not in base directory")
        try:
            os.makedirs(Path(path).parent, exist_ok=True)
            with open(path, "wb") as f:
                shutil.copyfileobj(file.file, f)
        finally:
            file.file.close()

    def post_file_url(
        self, path: str, request: Request, url_endpoint: str, files_endpoint: str
    ) -> FileHTTPRequest:
        if Path(self.base_post_path) not in Path(path).parents:
            raise PermissionError("Path is not in base directory")
        return FileHTTPRequest(
            url=re.sub(url_endpoint, files_endpoint, request.url._url),
            method="post",
            headers=(
                {"authorization": request.headers["authorization"]}
                if "authorization" in request.headers
                else {}
            ),
        )


class S3Filesystem(FileSystem):
    """Filesystem on S3."""

    def __init__(self, s3_client: S3Client, bucket: str):
        self.s3_client = s3_client
        self.bucket = bucket

    def get_file(self, path: str) -> FileResponse:
        raise PermissionError("Please get a pre-signed url instead.")

    def _get_bucket_path(self, path: str) -> tuple[str, str]:
        if not path.startswith("s3://"):
            raise PermissionError("Path should start with s3://")
        bucket, _, path = path[5:].partition("/")
        if not bucket == self.bucket:
            raise PermissionError("Bucket does not match")
        return bucket, path

    def get_file_url(
        self, path: str, request: Request, url_endpoint: str, files_endpoint: str
    ) -> FileHTTPRequest:
        bucket, _ = self._get_bucket_path(path)

        response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
        if "Contents" not in response:
            raise FileNotFoundError()

        return FileHTTPRequest(
            url=self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": path},
                ExpiresIn=60 * 10,
            ),
            method="get",
        )

    def post_file(self, file: UploadFile, path: str) -> None:
        raise PermissionError("Please get a pre-signed url instead.")
        # bucket, path = self._get_bucket_path(path)
        # self.s3_client.upload_fileobj(file.file, bucket, path)

    def post_file_url(
        self, path: str, request: Request, url_endpoint: str, files_endpoint: str
    ) -> FileHTTPRequest:
        bucket, path = self._get_bucket_path(path)
        if path[-1] != "/":
            path = path + "/"
        ret = self.s3_client.generate_presigned_post(
            Bucket=bucket,
            Key=path + "${filename}",
            Fields=None,
            Conditions=[
                ["starts-with", "$key", path]
            ],  # can be used for multiple uploads to folder
            ExpiresIn=60 * 10,
        )
        return FileHTTPRequest(
            url=ret["url"],
            method="post",
            data=ret["fields"],
        )
