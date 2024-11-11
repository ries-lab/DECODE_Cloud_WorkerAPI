from fastapi.testclient import TestClient

from workerfacing_api.core.filesystem import FileSystem
from workerfacing_api.main import workerfacing_app

client = TestClient(workerfacing_app)
endpoint = "/files"


def test_get_file(
    env: str,
    base_filesystem: FileSystem,
    data_file1: str,
    data_file1_name: str,
    data_file1_contents: str,
) -> None:
    if env == "local":
        file_resp = client.get(f"{endpoint}/{data_file1_name}/download")
        assert file_resp.content.decode("utf-8") == data_file1_contents
    else:
        file_resp = client.get(f"{endpoint}/{data_file1_name}/download")
        assert file_resp.status_code == 403


def test_get_file_not_exists(
    env: str, base_filesystem: FileSystem, data_file1: str
) -> None:
    file_resp = client.get(f"{endpoint}/not_exists")
    assert str(file_resp.status_code).startswith("4")


def test_get_file_url(
    env: str, base_filesystem: FileSystem, data_file1: str, data_file1_name: str
) -> None:
    req = f"{endpoint}/{data_file1_name}/url"
    url_resp = client.get(req)
    assert url_resp.status_code == 200
    if env == "local":
        assert req.replace("/url", "/download") in url_resp.text


def test_get_file_url_not_exists(
    env: str, base_filesystem: FileSystem, data_file1_name: str
) -> None:
    url_resp = client.get(f"{endpoint}/{data_file1_name}_fake/url")
    assert url_resp.status_code == 404


def test_get_file_url_not_permitted(
    env: str, base_filesystem: FileSystem, data_file1_name: str
) -> None:
    url_resp = client.get(f"{endpoint}/wrong_dir/url")
    assert url_resp.status_code == 403
