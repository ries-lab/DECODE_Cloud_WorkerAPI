import datetime
import os
import shutil
from io import BytesIO
from typing import Any, Generator, TypedDict, cast
from unittest.mock import MagicMock

import boto3
import dotenv
import pytest
from moto import mock_aws

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from workerfacing_api import settings
from workerfacing_api.core.filesystem import FileSystem, LocalFilesystem, S3Filesystem
from workerfacing_api.core.queue import JobQueue
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

test_username = "test_user"
example_app = AppSpecs(cmd=["cmd"], env={"env": "var"})
example_paths_upload = PathsUploadSpecs(
    output=f"{test_username}/out",
    log=f"{test_username}/log",
    artifact=f"{test_username}/artifact",
)


@pytest.fixture(scope="module")
def base_dir() -> str:
    return "test_user_dir"


@pytest.fixture(scope="module")
def data_file1_contents() -> str:
    return "data file1 contents"


@pytest.fixture(scope="module")
def internal_api_key_secret() -> str:
    return "test_internal_api_key"


@pytest.fixture(scope="module")
def monkeypatch_module() -> Generator[pytest.MonkeyPatch, Any, None]:
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(autouse=True, scope="function")
def patch_update_job(monkeypatch_module: pytest.MonkeyPatch) -> MagicMock:
    mock_update_job = MagicMock()
    monkeypatch_module.setattr(job_tracking, "update_job", mock_update_job)
    return mock_update_job


@pytest.fixture(
    scope="module",
    params=["local", "aws_mock", pytest.param("aws", marks=pytest.mark.aws)],
)
def env(request: pytest.FixtureRequest) -> str:
    assert isinstance(request.param, str)
    return request.param


@pytest.fixture(scope="module")
def base_filesystem(
    env: str, base_dir: str, monkeypatch_module: pytest.MonkeyPatch
) -> Generator[FileSystem, Any, None]:
    bucket_name = "decode-cloud-tests-bucket"
    region_name = "eu-central-1"

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
        yield LocalFilesystem(base_dir, base_dir)
        try:
            shutil.rmtree(base_dir)
        except FileNotFoundError:
            pass

    elif env == "aws_mock":
        with mock_aws():
            s3_client = boto3.client("s3", region_name=region_name)
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region_name},  # type: ignore
            )
            yield S3Filesystem(s3_client, bucket_name)

    elif env == "aws":
        s3_client = boto3.client("s3", region_name=region_name)
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region_name},  # type: ignore
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
def override_filesystem_dep(
    base_filesystem: FileSystem, monkeypatch_module: pytest.MonkeyPatch
) -> None:
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        filesystem_dep,
        lambda: base_filesystem,
    )


@pytest.fixture(autouse=True, scope="module")
def override_auth(monkeypatch_module: pytest.MonkeyPatch) -> None:
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
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
def override_internal_api_key_secret(
    monkeypatch_module: pytest.MonkeyPatch, internal_api_key_secret: str
) -> str:
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        authorizer,
        APIKeyDependency(internal_api_key_secret),
    )
    return internal_api_key_secret


@pytest.fixture
def require_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        current_user_dep,
    )


@pytest.fixture
def data_file1_name(
    env: str, base_dir: str, base_filesystem: FileSystem
) -> Generator[str, Any, None]:
    if env == "local":
        yield f"{base_dir}/data/test/data_file1.txt"
    else:
        base_filesystem = cast(S3Filesystem, base_filesystem)
        yield f"s3://{base_filesystem.bucket}/{base_dir}/data/test/data_file1.txt"


@pytest.fixture
def data_file1(
    env: str,
    base_filesystem: FileSystem,
    data_file1_name: str,
    data_file1_contents: str,
) -> None:
    file_name = data_file1_name
    if env == "local":
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, "w") as f:
            f.write(data_file1_contents)
    else:  # s3
        base_filesystem = cast(S3Filesystem, base_filesystem)
        base_filesystem.s3_client.put_object(
            Bucket=base_filesystem.bucket,
            Key=file_name,
            Body=BytesIO(data_file1_contents.encode("utf-8")),
        )


@pytest.fixture(scope="function")
def jobs(
    env: str, base_filesystem: str
) -> tuple[SubmittedJob, SubmittedJob, SubmittedJob, SubmittedJob]:
    time_now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    paths_upload = example_paths_upload.model_copy()
    for k in paths_upload.model_fields:
        if env == "local":
            path = f"{cast(LocalFilesystem, base_filesystem).base_post_path}/{getattr(paths_upload, k)}"
        else:
            path = f"s3://{cast(S3Filesystem, base_filesystem).bucket}/{getattr(paths_upload, k)}"
        setattr(paths_upload, k, path)

    JobBase = TypedDict(
        "JobBase", {"app": AppSpecs, "handler": HandlerSpecs, "hardware": HardwareSpecs}
    )
    common_job_base: JobBase = {
        "app": example_app,
        "handler": HandlerSpecs(image_url="u", files_up={"output": "out"}),
        "hardware": HardwareSpecs(),
    }
    job0 = SubmittedJob(
        job=JobSpecs(
            **common_job_base, meta=MetaSpecs(job_id=0, date_created=time_now)
        ),
        environment=EnvironmentTypes.local,
        paths_upload=paths_upload,
    )
    job1 = SubmittedJob(
        job=JobSpecs(
            **common_job_base, meta=MetaSpecs(job_id=1, date_created=time_now)
        ),
        environment=EnvironmentTypes.local,
        paths_upload=paths_upload,
    )
    job2 = SubmittedJob(
        job=JobSpecs(
            **common_job_base, meta=MetaSpecs(job_id=2, date_created=time_now)
        ),
        environment=EnvironmentTypes.any,
        paths_upload=paths_upload,
    )
    job3 = SubmittedJob(
        job=JobSpecs(
            **common_job_base, meta=MetaSpecs(job_id=3, date_created=time_now)
        ),
        environment=EnvironmentTypes.cloud,
        paths_upload=paths_upload,
    )
    return job0, job1, job2, job3


@pytest.fixture(scope="function")
def full_jobs(
    jobs: tuple[SubmittedJob, SubmittedJob, SubmittedJob, SubmittedJob],
) -> tuple[SubmittedJob, SubmittedJob, SubmittedJob, SubmittedJob]:
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
def populated_queue(
    queue: JobQueue, jobs: tuple[SubmittedJob, SubmittedJob, SubmittedJob, SubmittedJob]
) -> JobQueue:
    job0, job1, job2, job3 = jobs
    queue.enqueue(job0)
    queue.enqueue(job1)
    queue.enqueue(job2)
    queue.enqueue(job3)
    return queue


@pytest.fixture
def populated_full_queue(
    queue: JobQueue,
    full_jobs: tuple[SubmittedJob, SubmittedJob, SubmittedJob, SubmittedJob],
) -> JobQueue:
    job1, job2, job3, job4 = full_jobs
    queue.enqueue(job1)
    queue.enqueue(job2)
    queue.enqueue(job3)
    queue.enqueue(job4)
    return queue
