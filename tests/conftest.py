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
from workerfacing_api.schemas.queue_jobs import (
    AppSpecs,
    EnvironmentTypes,
    HandlerSpecs,
    HardwareSpecs,
    JobSpecs,
    MetaSpecs,
    PathsUploadSpecs,
    SubmittedJob,
)

base_dir = "test_user_dir"
test_username = "test_user"
example_app = AppSpecs(cmd=["cmd"], env={"env": "var"})
example_paths_upload = PathsUploadSpecs(
    output=f"{test_username}/out",
    log=f"{test_username}/log",
    artifact=f"{test_username}/artifact",
)


@pytest.fixture(scope="module")
def data_file1_contents():
    return "data file1 contents"


@pytest.fixture(scope="module")
def internal_api_key_secret():
    return "test_internal_api_key"


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
    time_now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    paths_upload = example_paths_upload.model_copy()
    for k in paths_upload.model_fields:
        if env == "local":
            path = f"{base_filesystem.base_post_path}/{getattr(paths_upload, k)}"
        else:
            path = f"s3://{base_filesystem.bucket}/{getattr(paths_upload, k)}"
        setattr(paths_upload, k, path)

    common_job_base = {
        "app": example_app,
        "handler": HandlerSpecs(image_url="u", files_up={"output": "out"}),
        "hardware": HardwareSpecs(),
    }
    common_base = {"paths_upload": paths_upload}
    job0 = SubmittedJob(
        job=JobSpecs(
            **common_job_base, meta=MetaSpecs(job_id=0, date_created=time_now)
        ),
        environment=EnvironmentTypes.local,
        **common_base,
    )
    job1 = SubmittedJob(
        job=JobSpecs(
            **common_job_base, meta=MetaSpecs(job_id=1, date_created=time_now)
        ),
        environment=EnvironmentTypes.local,
        **common_base,
    )
    job2 = SubmittedJob(
        job=JobSpecs(
            **common_job_base, meta=MetaSpecs(job_id=2, date_created=time_now)
        ),
        environment=EnvironmentTypes.any,
        **common_base,
    )
    job3 = SubmittedJob(
        job=JobSpecs(
            **common_job_base, meta=MetaSpecs(job_id=3, date_created=time_now)
        ),
        environment=EnvironmentTypes.cloud,
        **common_base,
    )
    return job0, job1, job2, job3


@pytest.fixture(scope="function")
def full_jobs(jobs):
    job0, job1, job2, job3 = [job.model_copy() for job in jobs]

    job0.job.hardware = HardwareSpecs(
        gpu_model="gpu_model",
        gpu_archi="gpu_archi",
    )
    job0.priority = 5

    job1.job.hardware = HardwareSpecs(
        cpu_cores=2,
        memory=0,
        gpu_model=None,
        gpu_archi=None,
        gpu_mem=None,
    )
    job1.priority = 10

    job2.group = "group"
    job2.priority = 1
    job2.environment = EnvironmentTypes.local

    job3.priority = 1
    job3.environment = EnvironmentTypes.local

    return job0, job1, job2, job3


@pytest.fixture
def populated_queue(queue, jobs):
    job0, job1, job2, job3 = jobs
    queue.enqueue(job0)
    queue.enqueue(job1)
    queue.enqueue(job2)
    queue.enqueue(job3)
    return queue


@pytest.fixture
def populated_full_queue(queue, full_jobs):
    job1, job2, job3, job4 = full_jobs
    queue.enqueue(job1)
    queue.enqueue(job2)
    queue.enqueue(job3)
    queue.enqueue(job4)
    return queue
