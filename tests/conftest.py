import datetime
import os
import shutil
from io import BytesIO
from unittest.mock import MagicMock

import dotenv
import pytest
from fastapi import UploadFile

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from workerfacing_api import settings
from workerfacing_api.crud import job_tracking
from workerfacing_api.dependencies import (
    APIKeyDependency,
    GroupClaims,
    authorizer,
    current_user_dep,
    filesystem_dep,
)
from workerfacing_api.main import workerfacing_app

base_dir = "test_user_dir"
test_username = "test_user"
example_app = {"application": "app", "version": "latest", "entrypoint": "test"}
example_paths_upload = {
    "output": f"{test_username}/out",
    "log": f"{test_username}/log",
    "artifact": f"{test_username}/artifact",
}


@pytest.fixture(scope="module")
def data_file1_contents():
    return "data file1 contents"


@pytest.fixture(scope="module")
def internal_api_key_secret():
    return "test_internal_api_key"


@pytest.fixture(autouse=True)
def env_name():
    return "local"


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
        from moto import mock_aws

        with mock_aws():
            import boto3

            from workerfacing_api.core.filesystem import S3Filesystem

            s3_client = boto3.client("s3", region_name=region_name)
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region_name},
            )
            yield S3Filesystem(s3_client, bucket_name)

    elif env == "aws":
        import boto3

        from workerfacing_api.core.filesystem import S3Filesystem

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
        lambda: GroupClaims(
            **{
                "cognito:username": test_username,
                "cognito:email": "test@example.com",
                "cognito:groups": ["workers"],
            }
        ),
    )


@pytest.fixture(scope="module", autouse=True)
def override_internal_api_key_secret(monkeypatch_module, internal_api_key_secret):
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
def data_file1(env, base_filesystem, data_file1_name, data_file1_contents):
    file_name = data_file1_name
    base_filesystem.post_file(
        UploadFile(filename="", file=BytesIO(bytes(data_file1_contents, "utf-8"))),
        file_name,
    )


@pytest.fixture(scope="function")
def jobs(env, base_filesystem):
    time_now = datetime.datetime.utcnow().isoformat()

    paths_upload = example_paths_upload.copy()
    for k, v in paths_upload.items():
        if env == "local":
            paths_upload[k] = f"{base_filesystem.base_post_path}/{v}"
        else:
            paths_upload[k] = f"s3://{base_filesystem.bucket}/{v}"

    common_base = {
        "app": example_app,
        "handler": {"image_url": "u", "files_up": {"output": "out"}},
        "hardware": {},
    }
    job0 = {
        "job": {**common_base, "meta": {"job_id": 0, "date_created": time_now}},
        "paths_upload": paths_upload,
    }
    job1 = {
        "job": {**common_base, "meta": {"job_id": 1, "date_created": time_now}},
        "paths_upload": paths_upload,
    }
    job2 = {
        "job": {**common_base, "meta": {"job_id": 2, "date_created": time_now}},
        "paths_upload": paths_upload,
    }
    job3 = {
        "job": {**common_base, "meta": {"job_id": 3, "date_created": time_now}},
        "paths_upload": paths_upload,
    }
    return job0, job1, job2, job3


@pytest.fixture(scope="function")
def full_jobs(jobs):
    job0, job1, job2, job3 = jobs

    job0["job"]["hardware"] = {
        "cpu_cores": 3,
        "memory": 2,
        "gpu_model": "gpu_model",
        "gpu_archi": "gpu_archi",
        "gpu_mem": 0,
    }
    job0.update({"group": None, "priority": 5})

    job1["job"]["hardware"] = {
        "cpu_cores": 1,
        "memory": 0,
        "gpu_model": None,
        "gpu_archi": None,
        "gpu_mem": None,
    }
    job1.update({"group": None, "priority": 10})

    job2.update({"group": "group", "priority": 1})

    job3.update({"priority": 1})

    return job0, job1, job2, job3


@pytest.fixture
def populated_queue(queue, jobs, env_name):
    job1, job2, job3, job4 = jobs
    queue.enqueue(environment=env_name, item=job1)
    queue.enqueue(environment=env_name, item=job2)
    queue.enqueue(environment=None, item=job3)
    queue.enqueue(environment=f"not-{env_name}", item=job4)
    return queue


@pytest.fixture
def populated_full_queue(queue, full_jobs, env_name):
    job1, job2, job3, job4 = full_jobs
    queue.enqueue(environment=env_name, item=job1)
    queue.enqueue(environment=env_name, item=job2)
    queue.enqueue(environment=env_name, item=job3)
    queue.enqueue(environment=env_name, item=job4)
    return queue
