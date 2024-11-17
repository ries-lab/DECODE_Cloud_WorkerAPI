import os
from io import BytesIO
from typing import cast

import pytest
import requests
from fastapi.testclient import TestClient

from tests.integration.endpoints.conftest import EndpointParams, _TestEndpoint
from workerfacing_api.core.filesystem import FileSystem, S3Filesystem


@pytest.fixture(scope="module")
def data_file1_name(base_dir: str) -> str:
    return f"{base_dir}/data/test/data_file1.txt"


@pytest.fixture(scope="module")
def data_file1_path(env: str, data_file1_name: str, base_filesystem: FileSystem) -> str:
    if env == "aws":
        base_filesystem = cast(S3Filesystem, base_filesystem)
        return f"s3://{base_filesystem.bucket}/{data_file1_name}"
    return data_file1_name


@pytest.fixture(scope="module")
def data_file1_contents() -> str:
    return "data_file1"


@pytest.fixture(scope="module", autouse=True)
def data_file1(
    env: str,
    base_filesystem: FileSystem,
    data_file1_name: str,
    data_file1_contents: str,
) -> None:
    if env == "local":
        os.makedirs(os.path.dirname(data_file1_name), exist_ok=True)
        with open(data_file1_name, "w") as f:
            f.write(data_file1_contents)
    else:
        base_filesystem = cast(S3Filesystem, base_filesystem)
        base_filesystem.s3_client.put_object(
            Bucket=base_filesystem.bucket,
            Key=data_file1_name,
            Body=BytesIO(data_file1_contents.encode("utf-8")),
        )


class TestFiles(_TestEndpoint):
    endpoint = "/files"

    @pytest.fixture(scope="module")
    def passing_params(self, data_file1_path: str) -> list[EndpointParams]:
        return [
            EndpointParams("get", f"{data_file1_path}/url"),
        ]

    def test_get_file(
        self,
        env: str,
        data_file1_path: str,
        data_file1_contents: str,
        client: TestClient,
    ) -> None:
        if env == "local":
            file_resp = client.get(f"{self.endpoint}/{data_file1_path}/download")
            assert file_resp.status_code == 200
            assert file_resp.content.decode("utf-8") == data_file1_contents
        else:
            file_resp = client.get(f"{self.endpoint}/{data_file1_path}/download")
            assert file_resp.status_code == 403

    def test_get_file_not_exists(
        self, data_file1_path: str, client: TestClient
    ) -> None:
        file_resp = client.get(f"{self.endpoint}/{data_file1_path}_not_exists")
        assert file_resp.status_code == 404

    def test_get_file_not_permitted(self, client: TestClient) -> None:
        file_resp = client.get(f"{self.endpoint}/wrong_dir/download")
        assert file_resp.status_code == 403

    def test_get_file_url(
        self,
        env: str,
        data_file1_path: str,
        data_file1_contents: str,
        client: TestClient,
    ) -> None:
        req = f"{self.endpoint}/{data_file1_path}/url"
        url_resp = client.get(req)
        assert url_resp.status_code == 200
        if env == "local":
            assert req.replace("/url", "/download") in url_resp.text
        else:
            assert (
                requests.request(**url_resp.json()).content.decode("utf-8")
                == data_file1_contents
            )

    def test_get_file_url_not_exists(
        self, data_file1_path: str, client: TestClient
    ) -> None:
        url_resp = client.get(f"{self.endpoint}/{data_file1_path}_fake/url")
        assert url_resp.status_code == 404

    def test_get_file_url_not_permitted(self, client: TestClient) -> None:
        url_resp = client.get(f"{self.endpoint}/wrong_dir/url")
        assert url_resp.status_code == 403
