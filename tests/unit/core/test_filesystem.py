from tests.conftest import base_filesystem, data_file1_name, data_file1_contents, data_file1, env
import pytest
import requests
from starlette.responses import FileResponse
from fastapi import HTTPException


def test_get_file(env, base_filesystem, data_file1, data_file1_name):
    if env == "local":
        file_resp = base_filesystem.get_file(data_file1_name)
        assert isinstance(file_resp, FileResponse)
    else:
        with pytest.raises(HTTPException):
            base_filesystem.get_file(data_file1_name)


def test_get_file_not_exists(env, base_filesystem, data_file1):
    with pytest.raises(HTTPException):
        base_filesystem.get_file("not_exists")


def test_get_file_url(env, base_filesystem, data_file1, data_file1_name):
    url = base_filesystem.get_file_url(
        data_file1_name, f"http://example.com/test_url/{data_file1_name}", "test_url", "files"
    )
    if env == "local":
        assert url == f"http://example.com/files/{data_file1_name}"
    elif env == "s3":
        resp = requests.get(url)
        assert resp == data_file1_contents


def test_get_file_url_not_exists(env, base_filesystem, data_file1_name):
    with pytest.raises(HTTPException):
        base_filesystem.get_file_url(
            data_file1_name + "_fake", f"http://example.com/test_url/not_exists", "test_url", "files"
        )
