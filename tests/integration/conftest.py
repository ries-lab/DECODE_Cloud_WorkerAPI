import shutil
from typing import Any, Generator

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


@pytest.fixture(scope="module")
def test_username() -> str:
    return "test_user"


@pytest.fixture(scope="module")
def base_dir() -> str:
    return "int_test_dir"


@pytest.fixture(scope="module")
def internal_api_key_secret() -> str:
    return "test_internal_api_key"


@pytest.fixture(
    scope="module",
    params=["local", pytest.param("aws", marks=pytest.mark.aws)],
)
def env(request: pytest.FixtureRequest) -> str:
    assert isinstance(request.param, str)
    return request.param


@pytest.fixture(scope="module")
def base_filesystem(
    env: str, base_dir: str, monkeypatch_module: pytest.MonkeyPatch
) -> Generator[FileSystem, Any, None]:
    bucket_name = "decode-cloud-integration-tests"

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
        shutil.rmtree(base_dir, ignore_errors=True)
        yield LocalFilesystem(base_dir, base_dir)
        shutil.rmtree(base_dir, ignore_errors=True)

    elif env == "aws":
        testing_bucket = S3TestingBucket(bucket_name)
        yield S3Filesystem(testing_bucket.s3_client, testing_bucket.bucket_name)
        testing_bucket.cleanup()

    else:
        raise NotImplementedError


@pytest.fixture(scope="module")
def queue(
    env: str, tmpdir_factory: pytest.TempdirFactory
) -> Generator[RDSJobQueue, Any, None]:
    if env == "local":
        queue = RDSJobQueue(
            f"sqlite:///{tmpdir_factory.mktemp('integration')}/local.db"
        )
    else:
        db = RDSTestingInstance("decodecloudintegrationtests")
        queue = RDSJobQueue(db.db_url)
    queue.create(err_on_exists=True)
    yield queue
    queue.delete()
    if env == "aws":
        db.cleanup()


@pytest.fixture(scope="module", autouse=True)
def override_filesystem_dep(
    base_filesystem: FileSystem, monkeypatch_module: pytest.MonkeyPatch
) -> None:
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        filesystem_dep,
        lambda: base_filesystem,
    )


@pytest.fixture(scope="module", autouse=True)
def override_queue_dep(
    queue: RDSJobQueue, monkeypatch_module: pytest.MonkeyPatch
) -> None:
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        queue_dep,
        lambda: queue,
    )


@pytest.fixture(scope="module", autouse=True)
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
