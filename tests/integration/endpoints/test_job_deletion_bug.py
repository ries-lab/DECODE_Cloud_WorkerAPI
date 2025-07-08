import datetime
from typing import cast

import pytest
from fastapi.testclient import TestClient

from tests.integration.endpoints.conftest import EndpointParams, _TestEndpoint
from workerfacing_api.core.filesystem import FileSystem, LocalFilesystem, S3Filesystem
from workerfacing_api.core.queue import RDSJobQueue
from workerfacing_api.crud import job_tracking
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


@pytest.fixture(scope="module")
def app() -> AppSpecs:
    return AppSpecs(cmd=["cmd"], env={"env": "var"})


@pytest.fixture(scope="module")
def handler() -> HandlerSpecs:
    return HandlerSpecs(image_url="u", files_up={"output": "out"})


@pytest.fixture(scope="module")
def paths_upload(
    env: str, test_username: str, base_filesystem: FileSystem
) -> PathsUploadSpecs:
    if env == "local":
        base_path = cast(LocalFilesystem, base_filesystem).base_post_path
    else:
        base_path = f"s3://{cast(S3Filesystem, base_filesystem).bucket}"
    return PathsUploadSpecs(
        output=f"{base_path}/{test_username}/test_out/1",
        log=f"{base_path}/{test_username}/test_log/1",
        artifact=f"{base_path}/{test_username}/test_arti/1",
    )


class TestJobDeletion(_TestEndpoint):
    """Test for bug when job is deleted in user-facing API."""
    
    endpoint = "/jobs"

    @pytest.fixture(scope="module")
    def passing_params(self) -> list[EndpointParams]:
        return [EndpointParams("get", params={"memory": 1})]

    @pytest.fixture(scope="function", autouse=True)
    def cleanup_queue(self, queue: RDSJobQueue) -> None:
        queue.delete()
        queue.create()

    @pytest.fixture(scope="function")
    def base_job(
        self, app: AppSpecs, handler: HandlerSpecs, paths_upload: PathsUploadSpecs
    ) -> SubmittedJob:
        time_now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        return SubmittedJob(
            job=JobSpecs(
                app=app,
                handler=handler,
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

    def test_put_job_status_when_job_deleted_by_user(
        self,
        queue: RDSJobQueue,
        base_job: SubmittedJob,
        client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that reproduces the bug when job is deleted in user-facing API."""
        # Arrange: Add job to queue
        queue.enqueue(base_job)
        
        # Act: Pull job (this should work)
        get_response = client.get(self.endpoint, params={"memory": 1})
        assert get_response.status_code == 200
        assert "1" in get_response.json()
        
        # Mock the job_tracking.update_job to simulate job deletion in user-facing API
        def mock_update_job(*args, **kwargs):
            raise ValueError("Job 1 not found; it was probably deleted by the user.")
        
        monkeypatch.setattr(job_tracking, "update_job", mock_update_job)
        
        # Act: Try to update job status
        put_response = client.put(f"{self.endpoint}/1/status", params={"status": "running"})
        
        # Assert: After fix, this should return 204 (success) instead of 404
        # This allows worker to continue processing without getting an error
        assert put_response.status_code == 204
        
        # Verify that the response is empty (204 No Content)
        assert put_response.content == b""