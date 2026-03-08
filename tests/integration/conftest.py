import datetime
import shutil
from typing import Generator, cast

import pytest
from mypy_boto3_s3 import S3Client

from tests.conftest import RDSTestingInstance, S3TestingBucket
from workerfacing_api.core.auth import APIKeyDependency, GroupClaims
from workerfacing_api.core.filesystem import FileSystem, LocalFilesystem, S3Filesystem
from workerfacing_api.core.queue import RDSJobQueue, SQLiteRDSJobQueue
from workerfacing_api.dependencies import (
    authorizer,
    current_user_dep,
    filesystem_dep,
    queue_dep,
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


@pytest.fixture(scope="session")
def test_username() -> str:
    return "test_user"


@pytest.fixture(scope="session")
def base_dir(tmp_path_factory: pytest.TempPathFactory) -> str:
    return str(tmp_path_factory.mktemp("int_test_dir"))


@pytest.fixture(scope="session")
def internal_api_key_secret() -> str:
    return "test_internal_api_key"


@pytest.fixture(
    scope="session",
    params=["local-fs", pytest.param("aws-fs", marks=pytest.mark.aws)],
)
def base_filesystem(
    base_dir: str,
    s3_testing_bucket: S3TestingBucket,
    request: pytest.FixtureRequest,
) -> FileSystem:
    if request.param == "local-fs":
        return LocalFilesystem(base_dir, base_dir)
    elif request.param == "aws-fs":
        s3_testing_bucket.create()
        return S3Filesystem(s3_testing_bucket.s3_client, s3_testing_bucket.bucket_name)
    else:
        raise NotImplementedError


@pytest.fixture(
    scope="session",
    params=["local-queue", pytest.param("aws-queue", marks=pytest.mark.aws)],
)
def queue(
    base_filesystem: FileSystem,
    s3_testing_bucket: S3TestingBucket,
    rds_testing_instance: RDSTestingInstance,
    tmpdir_factory: pytest.TempdirFactory,
    request: pytest.FixtureRequest,
) -> RDSJobQueue:
    retry_different = False  # allow retries on same worker
    if request.param == "local-queue":
        queue_path = tmpdir_factory.mktemp("integration") / "local.db"
        s3_bucket: str | None = None
        s3_client: S3Client | None = None
        if isinstance(base_filesystem, S3Filesystem):
            s3_bucket = s3_testing_bucket.bucket_name
            s3_client = s3_testing_bucket.s3_client
        return SQLiteRDSJobQueue(
            f"sqlite:///{queue_path}",
            retry_different=retry_different,
            s3_client=s3_client,
            s3_bucket=s3_bucket,
        )
    elif request.param == "aws-queue":
        if isinstance(base_filesystem, LocalFilesystem):
            pytest.skip("Only testing RDS queue in combination with S3 filesystem")
        rds_testing_instance.create()
        return RDSJobQueue(rds_testing_instance.db_url, retry_different=retry_different)
    else:
        raise NotImplementedError


@pytest.fixture(autouse=True)
def override_filesystem_dep(
    base_filesystem: FileSystem,
    s3_testing_bucket: S3TestingBucket,
    base_dir: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None, None, None]:
    monkeypatch.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        filesystem_dep,
        lambda: base_filesystem,
    )
    yield
    # cleanup after every test
    if isinstance(base_filesystem, S3Filesystem):
        s3_testing_bucket.cleanup()
    else:
        shutil.rmtree(base_dir, ignore_errors=True)


@pytest.fixture(autouse=True)
def override_queue_dep(
    queue: RDSJobQueue,
    rds_testing_instance: RDSTestingInstance,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None, None, None]:
    monkeypatch.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        queue_dep,
        lambda: queue,
    )
    yield
    if isinstance(queue, SQLiteRDSJobQueue):
        queue.delete()
    else:
        rds_testing_instance.cleanup()


@pytest.fixture(autouse=True)
def override_auth(monkeypatch: pytest.MonkeyPatch, test_username: str) -> None:
    monkeypatch.setitem(
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


@pytest.fixture(autouse=True)
def override_internal_api_key_secret(
    monkeypatch: pytest.MonkeyPatch, internal_api_key_secret: str
) -> str:
    monkeypatch.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        authorizer,
        APIKeyDependency(internal_api_key_secret),
    )
    return internal_api_key_secret


@pytest.fixture
def base_job(base_filesystem: FileSystem, test_username: str) -> SubmittedJob:
    time_now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if isinstance(base_filesystem, S3Filesystem):
        base_path = f"s3://{base_filesystem.bucket}"
    else:
        base_path = cast(LocalFilesystem, base_filesystem).base_post_path
    paths_upload = PathsUploadSpecs(
        output=f"{base_path}/{test_username}/test_out/1",
        log=f"{base_path}/{test_username}/test_log/1",
        artifact=f"{base_path}/{test_username}/test_arti/1",
    )
    return SubmittedJob(
        job=JobSpecs(
            app=AppSpecs(cmd=["cmd"], env={"env": "var"}),
            handler=HandlerSpecs(image_url="u", files_up={"output": "out"}),
            hardware=HardwareSpecs(),
            meta=MetaSpecs(
                job_id=1,
                date_created=time_now,
            ),
        ),
        environment=EnvironmentTypes.local,
        group=None,
        priority=1,
        paths_upload=paths_upload,
    )
