import io
import os
import shutil
from abc import ABC, abstractmethod
from contextlib import nullcontext
from io import BytesIO
from types import SimpleNamespace
from typing import Any, Generator, cast

import pytest
import requests
from fastapi import UploadFile
from moto import mock_aws
from starlette.requests import Request
from starlette.responses import FileResponse

from tests.conftest import S3TestingBucket
from workerfacing_api.core.filesystem import (
    FileSystem,
    LocalFilesystem,
    S3Filesystem,
)
from workerfacing_api.schemas.files import FileHTTPRequest


def _mock_request(url: str) -> Request:
    return cast(Request, SimpleNamespace(url=SimpleNamespace(_url=url), headers={}))


@pytest.fixture(scope="class")
def base_dir() -> str:
    return "fs_test_dir"


@pytest.fixture(scope="class")
def data_file1_name(base_dir: str) -> str:
    return f"{base_dir}/data/test/data_file1.txt"


@pytest.fixture(scope="class")
def data_file1_contents() -> str:
    return "data file1 contents"


class _TestFilesystem(ABC):
    @abstractmethod
    @pytest.fixture(scope="class")
    def base_filesystem(
        self, *args: Any, **kwargs: Any
    ) -> Generator[FileSystem, Any, None]:
        raise NotImplementedError

    @abstractmethod
    @pytest.fixture(scope="class", autouse=True)
    def data_file1(
        self,
        base_filesystem: FileSystem,
        data_file1_name: str,
        data_file1_contents: str,
    ) -> None:
        raise NotImplementedError

    @pytest.fixture(scope="class")
    def data_filepost_name(self, data_file1_name: str) -> str:
        return data_file1_name + "_post"

    @pytest.fixture(scope="class")
    def data_file1_path(self, base_filesystem: FileSystem, data_file1_name: str) -> str:
        return data_file1_name

    @pytest.fixture(scope="class")
    def data_filepost_path(
        self, base_filesystem: FileSystem, data_filepost_name: str
    ) -> str:
        return data_filepost_name

    def test_get_file(
        self,
        base_filesystem: FileSystem,
        data_file1_path: str,
    ) -> None:
        file_resp = base_filesystem.get_file(data_file1_path)
        assert isinstance(file_resp, FileResponse)

    def test_get_file_not_exists(
        self, base_filesystem: FileSystem, data_file1_path: str
    ) -> None:
        with pytest.raises(FileNotFoundError):
            base_filesystem.get_file(data_file1_path + "_wrong")

    def test_get_file_not_permitted(
        self, base_dir: str, base_filesystem: FileSystem, data_file1_path: str
    ) -> None:
        with pytest.raises(PermissionError):
            base_filesystem.get_file(data_file1_path.replace(base_dir, "wrong_dir"))

    def test_get_file_url(
        self,
        base_filesystem: FileSystem,
        data_file1_path: str,
        data_file1_contents: str,
    ) -> None:
        resp_url = base_filesystem.get_file_url(
            data_file1_path,
            _mock_request(f"http://example.com/test_url/{data_file1_path}"),
            "test_url",
            "files",
        )
        assert resp_url == FileHTTPRequest(
            method="get",
            url=f"http://example.com/files/{data_file1_path}",
        )

    def test_get_file_url_not_exists(
        self, base_filesystem: FileSystem, data_file1_path: str
    ) -> None:
        with pytest.raises(FileNotFoundError):
            base_filesystem.get_file_url(
                data_file1_path + "_fake",
                _mock_request("http://example.com/test_url/not_exists"),
                "test_url",
                "files",
            )

    def test_get_file_url_not_permitted(
        self, base_filesystem: FileSystem, data_file1_path: str
    ) -> None:
        fn_split = data_file1_path.split(os.path.sep)
        fn_split[-4] = fn_split[-4] + "_fake"
        fn_wrong = os.path.join(*fn_split)
        with pytest.raises(PermissionError):
            base_filesystem.get_file_url(
                fn_wrong,
                _mock_request(f"http://example.com/test_url/{data_file1_path}"),
                "test_url",
                "files",
            )

    def test_post_file(
        self,
        base_filesystem: FileSystem,
        data_filepost_path: str,
        data_file1_contents: str,
    ) -> None:
        base_filesystem.post_file(
            file=UploadFile(
                io.BytesIO(data_file1_contents.encode("utf-8")),
                filename=os.path.split(data_filepost_path)[-1],
            ),
            path=os.path.dirname(data_filepost_path),
        )
        # test file exists
        assert isinstance(base_filesystem.get_file(data_filepost_path), FileResponse)

    def test_post_file_not_permitted(
        self,
        base_dir: str,
        base_filesystem: FileSystem,
        data_filepost_path: str,
        data_file1_contents: str,
    ) -> None:
        with pytest.raises(PermissionError):
            base_filesystem.post_file(
                file=UploadFile(
                    io.BytesIO(data_file1_contents.encode("utf-8")),
                    filename=os.path.split(data_filepost_path)[-1],
                ),
                path=os.path.dirname(data_filepost_path).replace(base_dir, "wrong_dir"),
            )

    def test_post_file_url(
        self,
        base_filesystem: FileSystem,
        data_filepost_path: str,
        data_file1_contents: str,
    ) -> None:
        resp = base_filesystem.post_file_url(
            data_filepost_path,
            _mock_request(f"http://example.com/test_url/{data_filepost_path}"),
            "test_url",
            "files",
        )
        assert resp == FileHTTPRequest(
            method="post",
            url=f"http://example.com/files/{data_filepost_path}",
        )

    def test_post_file_url_not_permitted(
        self, base_filesystem: FileSystem, data_filepost_path: str
    ) -> None:
        fn_split = data_filepost_path.split(os.path.sep)
        fn_split[-4] = fn_split[-4] + "_fake"
        fn_wrong = os.path.join(*fn_split)
        with pytest.raises(PermissionError):
            base_filesystem.post_file_url(
                fn_wrong,
                _mock_request(f"http://example.com/test_url/{data_filepost_path}"),
                "test_url",
                "files",
            )


class TestLocalFilesystem(_TestFilesystem):
    @pytest.fixture(scope="class")
    def base_filesystem(self, base_dir: str) -> Generator[LocalFilesystem, Any, None]:
        yield LocalFilesystem(base_dir, base_dir)
        try:
            shutil.rmtree(base_dir)
        except FileNotFoundError:
            pass

    @pytest.fixture(scope="class", autouse=True)
    def data_file1(
        self,
        base_filesystem: FileSystem,
        data_file1_name: str,
        data_file1_contents: str,
    ) -> None:
        os.makedirs(os.path.dirname(data_file1_name), exist_ok=True)
        with open(data_file1_name, "w") as f:
            f.write(data_file1_contents)


class TestS3Filesystem(_TestFilesystem):
    bucket_name = "decode-cloud-filesystem-tests"

    @pytest.fixture(
        scope="class", params=[True, pytest.param(False, marks=pytest.mark.aws)]
    )
    def mock_aws_(self, request: pytest.FixtureRequest) -> bool:
        return cast(bool, request.param)

    @pytest.fixture(scope="class")
    def base_filesystem(
        self, mock_aws_: bool, bucket_suffix: str
    ) -> Generator[S3Filesystem, Any, None]:
        context_manager = mock_aws if mock_aws_ else nullcontext
        with context_manager():
            testing_bucket = S3TestingBucket(bucket_suffix)
            yield S3Filesystem(testing_bucket.s3_client, testing_bucket.bucket_name)
            testing_bucket.cleanup()

    @pytest.fixture(scope="class", autouse=True)
    def data_file1(
        self,
        base_filesystem: FileSystem,
        data_file1_name: str,
        data_file1_contents: str,
    ) -> None:
        base_filesystem = cast(S3Filesystem, base_filesystem)
        base_filesystem.s3_client.put_object(
            Bucket=base_filesystem.bucket,
            Key=data_file1_name,
            Body=BytesIO(data_file1_contents.encode("utf-8")),
        )

    @pytest.fixture(scope="class")
    def data_file1_path(self, base_filesystem: FileSystem, data_file1_name: str) -> str:
        return f"s3://{cast(S3Filesystem, base_filesystem).bucket}/{data_file1_name}"

    @pytest.fixture(scope="class")
    def data_filepost_path(
        self, base_filesystem: FileSystem, data_filepost_name: str
    ) -> str:
        return f"s3://{cast(S3Filesystem, base_filesystem).bucket}/{data_filepost_name}"

    def test_get_file(
        self,
        base_filesystem: FileSystem,
        data_file1_path: str,
    ) -> None:
        # direct get_file not allowed for S3
        with pytest.raises(PermissionError):
            base_filesystem.get_file(data_file1_path)

    def test_get_file_not_exists(
        self, base_filesystem: FileSystem, data_file1_path: str
    ) -> None:
        with pytest.raises(PermissionError):
            base_filesystem.get_file(data_file1_path + "_wrong")

    def test_get_file_url(
        self,
        base_filesystem: FileSystem,
        data_file1_path: str,
        data_file1_contents: str,
    ) -> None:
        resp_url = base_filesystem.get_file_url(
            data_file1_path,
            _mock_request(f"http://example.com/test_url/{data_file1_path}"),
            "test_url",
            "files",
        )
        resp = requests.request(**resp_url.model_dump())
        assert resp.content.decode("utf-8") == data_file1_contents

    def test_post_file(
        self,
        base_filesystem: FileSystem,
        data_filepost_path: str,
        data_file1_contents: str,
    ) -> None:
        with pytest.raises(PermissionError):
            super().test_post_file(
                base_filesystem,
                data_filepost_path,
                data_file1_contents,
            )

    def test_post_file_url(
        self,
        base_filesystem: FileSystem,
        data_filepost_path: str,
        data_file1_contents: str,
    ) -> None:
        resp = base_filesystem.post_file_url(
            os.path.dirname(data_filepost_path),
            _mock_request(f"http://example.com/test_url/{data_filepost_path}"),
            "test_url",
            "files",
        )
        file_post_resp = requests.request(
            **resp.model_dump(),
            files={
                "file": (
                    os.path.split(data_filepost_path)[-1],
                    io.BytesIO(data_file1_contents.encode("utf-8")),
                )
            },
        )
        file_post_resp.raise_for_status()
        resp = base_filesystem.get_file_url(
            data_filepost_path,
            _mock_request(f"http://example.com/test_url/{data_filepost_path}"),
            "test_url",
            "files",
        )
        assert (
            requests.request(**resp.model_dump()).content.decode("utf-8")
            == data_file1_contents
        )
