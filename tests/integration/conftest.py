import shutil
from typing import Any, Generator, cast

import pytest

from tests.conftest import RDSTestingInstance, S3TestingBucket
from workerfacing_api import settings
from workerfacing_api.core.filesystem import FileSystem, LocalFilesystem, S3Filesystem
from workerfacing_api.core.queue import RDSJobQueue
from workerfacing_api.dependencies import (
    APIKeyDependency,
    GroupClaims,
    authorizer,
    current_user_dep,
    filesystem_dep,
    queue_dep,
)
from workerfacing_api.main import workerfacing_app


@pytest.fixture(scope="session")
def test_username() -> str:
    return "test_user"


@pytest.fixture(scope="session")
def base_dir() -> str:
    return "int_test_dir"


@pytest.fixture(scope="session")
def internal_api_key_secret() -> str:
    return "test_internal_api_key"


@pytest.fixture(
    scope="session",
    params=["local", pytest.param("aws", marks=pytest.mark.aws)],
)
def env(
    request: pytest.FixtureRequest,
    rds_testing_instance: RDSTestingInstance,
    s3_testing_bucket: S3TestingBucket,
) -> Generator[str, Any, None]:
    env = cast(str, request.param)
    if env == "aws":
        rds_testing_instance.create()
        s3_testing_bucket.create()
    yield env
    if env == "aws":
        rds_testing_instance.cleanup()
        s3_testing_bucket.cleanup()


@pytest.fixture(scope="session")
def base_filesystem(
    env: str,
    base_dir: str,
    monkeypatch_module: pytest.MonkeyPatch,
    s3_testing_bucket: S3TestingBucket,
) -> Generator[FileSystem, Any, None]:
    monkeypatch_module.setattr(
        settings,
        "user_data_root_path",
        base_dir,
    )
    monkeypatch_module.setattr(
        settings,
        "filesystem",
        "local" if env == "local" else "s3",
    )

    if env == "local":
        shutil.rmtree(base_dir, ignore_errors=True)
        yield LocalFilesystem(base_dir, base_dir)
        shutil.rmtree(base_dir, ignore_errors=True)

    elif env == "aws":
        # Update settings to use the actual unique bucket name created by S3TestingBucket
        monkeypatch_module.setattr(
            settings,
            "s3_bucket",
            s3_testing_bucket.bucket_name,
        )
        yield S3Filesystem(s3_testing_bucket.s3_client, s3_testing_bucket.bucket_name)
        s3_testing_bucket.cleanup()

    else:
        raise NotImplementedError


@pytest.fixture(scope="session")
def queue(
    env: str,
    rds_testing_instance: RDSTestingInstance,
    tmpdir_factory: pytest.TempdirFactory,
) -> Generator[RDSJobQueue, Any, None]:
    if env == "local":
        queue = RDSJobQueue(
            f"sqlite:///{tmpdir_factory.mktemp('integration')}/local.db"
        )
    else:
        queue = RDSJobQueue(rds_testing_instance.db_url)
    queue.create(err_on_exists=True)
    yield queue
    queue.delete()


@pytest.fixture(scope="session", autouse=True)
def override_filesystem_dep(
    base_filesystem: FileSystem, monkeypatch_module: pytest.MonkeyPatch
) -> None:
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        filesystem_dep,
        lambda: base_filesystem,
    )


@pytest.fixture(scope="session", autouse=True)
def override_queue_dep(
    queue: RDSJobQueue, monkeypatch_module: pytest.MonkeyPatch
) -> None:
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        queue_dep,
        lambda: queue,
    )


@pytest.fixture(scope="session", autouse=True)
def override_auth(monkeypatch_module: pytest.MonkeyPatch, test_username: str) -> None:
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


@pytest.fixture(scope="session", autouse=True)
def override_internal_api_key_secret(
    monkeypatch_module: pytest.MonkeyPatch, internal_api_key_secret: str
) -> str:
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        authorizer,
        APIKeyDependency(internal_api_key_secret),
    )
    return internal_api_key_secret
