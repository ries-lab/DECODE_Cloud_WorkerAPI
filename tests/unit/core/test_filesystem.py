from types import SimpleNamespace

import pytest
import requests
from starlette.responses import FileResponse


def _mock_request(url):
    return SimpleNamespace(url=SimpleNamespace(_url=url), headers={})


def test_get_file(env, base_filesystem, data_file1, data_file1_name):
    if env == "local":
        file_resp = base_filesystem.get_file(data_file1_name)
        assert isinstance(file_resp, FileResponse)
    else:
        with pytest.raises(PermissionError):
            base_filesystem.get_file(data_file1_name)


def test_get_file_not_exists(env, base_filesystem, data_file1_name):
    error_cls = FileNotFoundError if env == "local" else PermissionError
    with pytest.raises(error_cls):
        base_filesystem.get_file(data_file1_name + "_wrong")


def test_get_file_not_permitted(env, base_dir, base_filesystem, data_file1_name):
    with pytest.raises(PermissionError):
        base_filesystem.get_file(data_file1_name.replace(base_dir, "wrong_dir"))


def test_get_file_url(
    env, base_filesystem, data_file1, data_file1_name, data_file1_contents
):
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
        assert resp == data_file1_contents


def test_get_file_url_not_exists(env, base_filesystem, data_file1_name):
    with pytest.raises(FileNotFoundError):
        base_filesystem.get_file_url(
            data_file1_name + "_fake",
            _mock_request("http://example.com/test_url/not_exists"),
            "test_url",
            "files",
        )


def test_get_file_url_not_permitted(env, base_dir, base_filesystem, data_file1_name):
    with pytest.raises(PermissionError):
        if env == "local":
            base_filesystem.get_file_url(
                data_file1_name.replace(base_dir, "wrong_dir"),
                _mock_request(f"http://example.com/test_url/{data_file1_name}"),
                "test_url",
                "files",
            )
        else:
            base_filesystem.get_file_url(
                data_file1_name.replace(base_filesystem.bucket, "wrong_bucket"),
                _mock_request(f"http://example.com/test_url/{data_file1_name}"),
                "test_url",
                "files",
            )


# TODO: test post_file/post_file_url
