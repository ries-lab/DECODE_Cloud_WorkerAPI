from types import SimpleNamespace
from typing import cast

import pytest
import requests
from starlette.requests import Request
from starlette.responses import FileResponse

from workerfacing_api.core.filesystem import FileSystem, LocalFilesystem, S3Filesystem


def _mock_request(url: str) -> Request:
    return cast(Request, SimpleNamespace(url=SimpleNamespace(_url=url), headers={}))


def test_get_file(
    env: str, base_filesystem: FileSystem, data_file1: str, data_file1_name: str
) -> None:
    if env == "local":
        file_resp = base_filesystem.get_file(data_file1_name)
        assert isinstance(file_resp, FileResponse)
    else:
        with pytest.raises(PermissionError):
            base_filesystem.get_file(data_file1_name)


def test_get_file_not_exists(
    env: str, base_filesystem: FileSystem, data_file1_name: str
) -> None:
    error_cls = FileNotFoundError if env == "local" else PermissionError
    with pytest.raises(error_cls):
        base_filesystem.get_file(data_file1_name + "_wrong")


def test_get_file_not_permitted(
    env: str, base_dir: str, base_filesystem: FileSystem, data_file1_name: str
) -> None:
    with pytest.raises(PermissionError):
        base_filesystem.get_file(data_file1_name.replace(base_dir, "wrong_dir"))


def test_get_file_url(
    env: str,
    base_filesystem: FileSystem,
    data_file1: str,
    data_file1_name: str,
    data_file1_contents: str,
) -> None:
    url = base_filesystem.get_file_url(
        data_file1_name,
        _mock_request(f"http://example.com/test_url/{data_file1_name}"),
        "test_url",
        "files",
    ).url
    if env == "local":
        assert url == f"http://example.com/files/{data_file1_name}"
    elif env == "s3":
        resp = requests.get(url)
        assert resp == data_file1_contents  # type: ignore


def test_get_file_url_not_exists(
    env: str, base_filesystem: FileSystem, data_file1_name: str
) -> None:
    with pytest.raises(FileNotFoundError):
        base_filesystem.get_file_url(
            data_file1_name + "_fake",
            _mock_request("http://example.com/test_url/not_exists"),
            "test_url",
            "files",
        )


def test_get_file_url_not_permitted(
    env: str, base_dir: str, base_filesystem: FileSystem, data_file1_name: str
) -> None:
    with pytest.raises(PermissionError):
        if env == "local":
            cast(LocalFilesystem, base_filesystem).get_file_url(
                data_file1_name.replace(base_dir, "wrong_dir"),
                _mock_request(f"http://example.com/test_url/{data_file1_name}"),
                "test_url",
                "files",
            )
        else:
            base_filesystem.get_file_url(
                data_file1_name.replace(
                    cast(S3Filesystem, base_filesystem).bucket, "wrong_bucket"
                ),
                _mock_request(f"http://example.com/test_url/{data_file1_name}"),
                "test_url",
                "files",
            )


# TODO: test post_file/post_file_url
