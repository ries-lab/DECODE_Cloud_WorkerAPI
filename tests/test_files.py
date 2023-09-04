from io import BytesIO

import pytest
from fastapi.testclient import TestClient
from api.main import app
from api.core.filesystem import get_user_filesystem
from .conftest import root_file1_name, root_file1_contents, root_file2_name, root_file2_contents, subdir_name, \
    subdir_file1_name, subdir_file1_contents, test_username
import api.settings as settings


client = TestClient(app)
endpoint = "/files"


@pytest.fixture(autouse=True)
def set_filesystem_env(monkeypatch):
    monkeypatch.setattr(settings, "filesystem", "local")


@pytest.fixture
def filesystem():
    filesystem = get_user_filesystem(test_username)
    yield filesystem
    filesystem.delete('/', reinit_if_root=False)


def test_auth_required(require_auth):
    original_overrides = app.dependency_overrides
    app.dependency_overrides = {}
    response = client.get(endpoint)
    assert response.status_code == 401
    app.dependency_overrides = original_overrides


def test_get_files_happy(multiple_files):
    response = client.get(endpoint)
    assert response.status_code == 200
    assert len(response.json()) == 3
    assert {"path": root_file1_name, "type": "file", "size": f"{len(root_file1_contents)} Bytes"} in response.json()
    assert {"path": root_file2_name, "type": "file", "size": f"{len(root_file2_contents)} Bytes"} in response.json()
    assert {"path": subdir_name, "type": "directory", "size": ""} in response.json()


def test_get_files_subdir_happy(multiple_files):
    response = client.get(f"{endpoint}/{subdir_name}")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert {"path": subdir_file1_name, "type": "file", "size": f"{len(subdir_file1_contents)} Bytes"} in response.json()


def test_get_files_fail_not_a_directory():
    response = client.get(f"{endpoint}/does_not_exist")
    assert response.status_code == 404


def test_post_files_happy(cleanup_files):
    files = {"file": (root_file1_name, BytesIO(bytes(root_file1_contents, 'utf-8')), "text/plain")}
    response = client.post(f"{endpoint}/{root_file1_name}", files=files)
    assert response.status_code == 201
    assert response.json() == {"path": root_file1_name, "type": "file", "size": f"{len(root_file1_contents)} Bytes"}
    cleanup_files.append(root_file1_name)
    response = client.get(endpoint)
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert {"path": root_file1_name, "type": "file", "size": f"{len(root_file1_contents)} Bytes"} in response.json()


def rename_test_implementation(response, cleanup_files):
    assert response.status_code == 200
    assert response.json() == {"path": root_file2_name, "type": "file", "size": f"{len(root_file1_contents)} Bytes"}
    cleanup_files.append(root_file2_name)
    response = client.get(endpoint)
    assert response.status_code == 200
    assert {"path": root_file2_name, "type": "file", "size": f"{len(root_file1_contents)} Bytes"} in response.json()


def test_put_files_happy(single_file, cleanup_files):
    response = client.put(f"{endpoint}/{root_file1_name}", json={"path": root_file2_name})
    rename_test_implementation(response, cleanup_files)


def test_put_files_fail_is_a_directory(multiple_files):
    response = client.put(f"{endpoint}/{subdir_name}", json={"path": root_file2_name})
    assert response.status_code == 400


def test_patch_files_happy(single_file, cleanup_files):
    response = client.patch(f"{endpoint}/{root_file1_name}", json={"path": root_file2_name})
    rename_test_implementation(response, cleanup_files)


def test_delete_files_happy(single_file):
    response = client.delete(f"{endpoint}/{root_file1_name}")
    assert response.status_code == 204
    response = client.get(endpoint)
    assert response.status_code == 200
    assert response.json() == []
