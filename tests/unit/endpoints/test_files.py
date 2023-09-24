from tests.conftest import base_filesystem, data_file1_name, data_file1_contents, data_file1, env
import pytest
import requests
from fastapi import HTTPException
from fastapi.testclient import TestClient
from workerfacing_api.main import workerfacing_app


client = TestClient(workerfacing_app)
endpoint = "/files"
endpoint_url = "/files_url"


def test_get_file(env, base_filesystem, data_file1, data_file1_name):
    if env == "local":
        file_resp = client.get(f"{endpoint}/{data_file1_name}")
        assert file_resp.content.decode("utf-8") == data_file1_contents
    else:
        with pytest.raises(HTTPException):
            base_filesystem.get_file(data_file1_name)


def test_get_file_not_exists(env, base_filesystem, data_file1):
    file_resp = client.get(f"{endpoint}/not_exists")
    assert str(file_resp.status_code).startswith("4")


def test_get_file_url(env, base_filesystem, data_file1, data_file1_name):
    url_resp = client.get(f"{endpoint_url}/{data_file1_name}")
    if env == "local":
        assert True
    elif env == "s3":
        resp = requests.get(url_resp)
        assert resp == data_file1_contents


def test_get_file_url_not_exists(env, base_filesystem, data_file1_name):
    url_resp = client.get(f"{endpoint_url}/not_exists")
    assert url_resp.status_code == 404
