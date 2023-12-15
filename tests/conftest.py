import dotenv
import os

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import pytest
import shutil
from fastapi import UploadFile
from io import BytesIO
from unittest.mock import MagicMock

from workerfacing_api import settings
from workerfacing_api.crud import job_tracking
from workerfacing_api.main import workerfacing_app
from workerfacing_api.dependencies import (
    current_user_dep,
    CognitoClaims,
    filesystem_dep,
    APIKeyDependency,
    authorizer,
)


base_dir = "test_user_dir"
data_file1_contents = f"data file1 contents"

test_username = "test_user"
internal_api_key_secret = "test_internal_api_key"

example_app = {"application": "app", "version": "latest", "entrypoint": "test"}
example_paths_upload = {
    "output": f"{test_username}/out",
    "log": f"{test_username}/log",
    "artifact": f"{test_username}/artifact",
}


@pytest.fixture(scope="module")
def monkeypatch_module():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(autouse=True, scope="function")
def patch_update_job(monkeypatch_module):
    mock_update_job = MagicMock()
    monkeypatch_module.setattr(job_tracking, "update_job", mock_update_job)
    return mock_update_job


@pytest.fixture(
    scope="module",
    params=["local", "aws_mock", pytest.param("aws", marks=pytest.mark.aws)],
)
def env(request):
    return request.param


@pytest.fixture(scope="module")
def base_filesystem(env, monkeypatch_module):
    bucket_name = "decode-cloud-tests-bucket"
    region_name = "eu-central-1"
    global base_dir

    monkeypatch_module.setattr(
        settings,
        "user_data_root_path",
        base_dir,
    )
    monkeypatch_module.setattr(
        settings,
        "s3_bucket",
        bucket_name,
    )
    monkeypatch_module.setattr(
        settings,
        "filesystem",
        "local" if env == "local" else "s3",
    )

    if env == "local":
        from workerfacing_api.core.filesystem import LocalFilesystem

        yield LocalFilesystem(base_dir, base_dir)
        try:
            shutil.rmtree(base_dir)
        except FileNotFoundError:
            pass

    elif env == "aws_mock":
        from moto import mock_s3

        with mock_s3():
            from workerfacing_api.core.filesystem import S3Filesystem
            import boto3

            s3_client = boto3.client("s3", region_name=region_name)
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region_name},
            )
            yield S3Filesystem(s3_client, bucket_name)

    elif env == "aws":
        from workerfacing_api.core.filesystem import S3Filesystem
        import boto3

        s3_client = boto3.client("s3", region_name=region_name)
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region_name},
        )
        yield S3Filesystem(s3_client, bucket_name)
        s3 = boto3.resource("s3", region_name=region_name)
        s3_bucket = s3.Bucket(bucket_name)
        bucket_versioning = s3.BucketVersioning(bucket_name)
        if bucket_versioning.status == "Enabled":
            s3_bucket.object_versions.delete()
        else:
            s3_bucket.objects.all().delete()
        s3_bucket.delete()

    else:
        raise NotImplementedError


@pytest.fixture(scope="module", autouse=True)
def override_filesystem_dep(base_filesystem, monkeypatch_module):
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides, filesystem_dep, lambda: base_filesystem
    )


@pytest.fixture(autouse=True, scope="module")
def override_auth(monkeypatch_module):
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,
        current_user_dep,
        lambda: CognitoClaims(
            **{"cognito:username": test_username, "email": "test@example.com"}
        ),
    )


@pytest.fixture(scope="module", autouse=True)
def override_internal_api_key_secret(monkeypatch_module):
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,
        authorizer,
        APIKeyDependency(internal_api_key_secret),
    )
    return internal_api_key_secret


@pytest.fixture
def require_auth(monkeypatch):
    monkeypatch.delitem(workerfacing_app.dependency_overrides, current_user_dep)


@pytest.fixture
def data_file1_name(env, base_filesystem):
    if env == "local":
        yield f"{base_dir}/data/test/data_file1.txt"
    else:
        yield f"s3://{base_filesystem.bucket}/{base_dir}/data/test/data_file1.txt"


@pytest.fixture
def data_file1(env, base_filesystem, data_file1_name):
    file_name = data_file1_name
    base_filesystem.post_file(
        UploadFile(filename="", file=BytesIO(bytes(data_file1_contents, "utf-8"))),
        file_name,
    )
