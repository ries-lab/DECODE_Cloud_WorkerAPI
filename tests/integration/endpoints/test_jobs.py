import datetime
import os
import time
from io import BytesIO
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
import requests
from fastapi.testclient import TestClient

from tests.conftest import RDSTestingInstance
from tests.integration.endpoints.conftest import EndpointParams, _TestEndpoint
from workerfacing_api.core.filesystem import FileSystem, LocalFilesystem, S3Filesystem
from workerfacing_api.core.queue import RDSJobQueue
from workerfacing_api.crud import job_tracking
from workerfacing_api.exceptions import JobDeletedException
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
from workerfacing_api.schemas.rds_models import JobStates


@pytest.fixture(scope="session")
def app() -> AppSpecs:
    return AppSpecs(cmd=["cmd"], env={"env": "var"})


@pytest.fixture(scope="session")
def handler() -> HandlerSpecs:
    return HandlerSpecs(image_url="u", files_up={"output": "out"})


@pytest.fixture(scope="session")
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


class TestJobs(_TestEndpoint):
    endpoint = "/jobs"

    @pytest.fixture(scope="session")
    def passing_params(self) -> list[EndpointParams]:
        return [EndpointParams("get", params={"memory": 1})]

    @pytest.fixture(scope="function", autouse=True)
    def cleanup_queue(
        self,
        queue: RDSJobQueue,
        env: str,
        rds_testing_instance: RDSTestingInstance,
    ) -> None:
        if env == "local":
            queue.delete()
        else:
            rds_testing_instance.cleanup()
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

    def test_get_jobs(
        self,
        queue: RDSJobQueue,
        base_job: SubmittedJob,
        patch_update_job: MagicMock,
        client: TestClient,
    ) -> None:
        queue.enqueue(base_job)
        resp = client.get(self.endpoint, params={"memory": 1})
        assert resp.status_code == 200, resp.json()
        assert resp.json() == {"1": base_job.job.model_dump()}
        patch_update_job.assert_called_with(1, JobStates.pulled, None)

    def test_get_jobs_required_params(self, client: TestClient) -> None:
        required = ["memory"]
        base_query_params = {"memory": 1}
        assert client.get(self.endpoint, params=base_query_params).status_code == 200
        for param in required:
            query_params = base_query_params.copy()
            del query_params[param]
            res = client.get(self.endpoint, params=query_params)
            assert res.status_code == 422
            assert "missing" in res.json()["detail"][0]["type"]

    def test_get_jobs_filtering_cpu_cores(
        self, queue: RDSJobQueue, base_job: SubmittedJob, client: TestClient
    ) -> None:
        base_job.job.hardware.cpu_cores = 2
        queue.enqueue(base_job)
        res = client.get(
            self.endpoint,
            params={"memory": 1},
        )
        assert res.json() == {}
        res = client.get(
            self.endpoint,
            params={"memory": 1, "cpu_cores": 1},
        )
        assert res.json() == {}
        res = client.get(
            self.endpoint,
            params={"memory": 1, "cpu_cores": 2},
        )
        assert res.json() == {"1": base_job.job.model_dump()}

    def test_get_jobs_filtering_memory(
        self, queue: RDSJobQueue, base_job: SubmittedJob, client: TestClient
    ) -> None:
        base_job.job.hardware.memory = 2
        queue.enqueue(base_job)
        res = client.get(
            self.endpoint,
            params={"memory": 1},
        )
        assert res.json() == {}
        res = client.get(
            self.endpoint,
            params={"memory": 2},
        )
        assert res.json() == {"1": base_job.job.model_dump()}

    def test_get_jobs_filtering_gpu_model(
        self, queue: RDSJobQueue, base_job: SubmittedJob, client: TestClient
    ) -> None:
        base_job.job.hardware.gpu_model = "gpu_model"
        queue.enqueue(base_job)
        res = client.get(
            self.endpoint,
            params={"memory": 1},
        )
        assert res.json() == {}
        res = client.get(
            self.endpoint,
            params={"memory": 1, "gpu_model": "another_gpu_model"},
        )
        res = client.get(
            self.endpoint,
            params={"memory": 1, "gpu_model": "gpu_model"},
        )
        assert res.json() == {"1": base_job.job.model_dump()}

    def test_get_jobs_filtering_gpu_archi(
        self, queue: RDSJobQueue, base_job: SubmittedJob, client: TestClient
    ) -> None:
        base_job.job.hardware.gpu_archi = "gpu_archi"
        queue.enqueue(base_job)
        res = client.get(
            self.endpoint,
            params={"memory": 1},
        )
        assert res.json() == {}
        res = client.get(
            self.endpoint,
            params={"memory": 1, "gpu_archi": "another_gpu_archi"},
        )
        res = client.get(
            self.endpoint,
            params={"memory": 1, "gpu_archi": "gpu_archi"},
        )
        assert res.json() == {"1": base_job.job.model_dump()}

    def test_get_jobs_filtering_gpu_mem(
        self, queue: RDSJobQueue, base_job: SubmittedJob, client: TestClient
    ) -> None:
        base_job.job.hardware.gpu_mem = 2
        queue.enqueue(base_job)
        res = client.get(
            self.endpoint,
            params={"memory": 1},
        )
        assert res.json() == {}
        res = client.get(
            self.endpoint,
            params={"memory": 1, "gpu_mem": 1},
        )
        assert res.json() == {}
        res = client.get(
            self.endpoint,
            params={"memory": 1, "gpu_mem": 2},
        )
        assert res.json() == {"1": base_job.job.model_dump()}

    def test_get_jobs_priorities(
        self, queue: RDSJobQueue, base_job: SubmittedJob, client: TestClient
    ) -> None:
        job_lower_priority = base_job.model_copy(update={"priority": 5})
        job_own_group = base_job.model_copy(update={"priority": 1, "group": "group"})
        job_higher_priority = base_job.model_copy(update={"priority": 10})
        queue.enqueue(job_lower_priority)
        queue.enqueue(job_own_group)
        queue.enqueue(job_higher_priority)
        res = client.get(
            self.endpoint,
            params={"groups": ["group"], "memory": 1, "limit": 1},
        )
        assert res.json() == {"2": job_own_group.job.model_dump()}
        res = client.get(
            self.endpoint,
            params={"groups": ["group"], "memory": 1, "limit": 1},
        )
        assert res.json() == {"3": job_higher_priority.job.model_dump()}

    def test_get_jobs_dequeue_old(
        self, queue: RDSJobQueue, base_job: SubmittedJob, client: TestClient
    ) -> None:
        queue.enqueue(base_job)
        queue.enqueue(base_job.model_copy(update={"environment": EnvironmentTypes.any}))
        # older_than does not apply when the right environment is selected
        res = client.get(self.endpoint, params={"memory": 1, "older_than": 5})
        assert "1" in res.json() and len(res.json()) == 1
        # not old enough
        res = client.get(self.endpoint, params={"memory": 1, "older_than": 5})
        assert res.json() == {}
        time.sleep(5)
        # old enough
        res = client.get(self.endpoint, params={"memory": 1, "older_than": 5})
        assert "2" in res.json() and len(res.json()) == 1

    def test_get_job_status(
        self, queue: RDSJobQueue, base_job: SubmittedJob, client: TestClient
    ) -> None:
        queue.enqueue(base_job)
        res = client.get(f"{self.endpoint}/1/status")
        assert res.json() == "queued"
        # dequeue
        res = client.get(self.endpoint, params={"memory": 1})
        res = client.get(f"{self.endpoint}/{list(res.json().keys())[0]}/status")
        assert res.json() == "pulled"

    def test_put_job_status(
        self, queue: RDSJobQueue, base_job: SubmittedJob, client: TestClient
    ) -> None:
        queue.enqueue(base_job)
        # job needs to be pulled in order to update its status
        res = client.put(f"{self.endpoint}/1/status", params={"status": "running"})
        assert res.status_code == 404
        client.get(self.endpoint, params={"memory": 1})
        res = client.put(f"{self.endpoint}/1/status", params={"status": "running"})
        assert res.status_code == 204
        res = client.get(f"{self.endpoint}/1/status")
        assert res.json() == "running"

    def test_put_job_status_canceled(
        self,
        queue: RDSJobQueue,
        base_job: SubmittedJob,
        client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        queue.enqueue(base_job)
        client.get(self.endpoint, params={"memory": 1})

        def mock_update_job(*args: Any, **kwargs: Any) -> None:
            raise JobDeletedException("Job not found")

        monkeypatch.setattr(job_tracking, "update_job", mock_update_job)
        res = client.put(f"{self.endpoint}/1/status", params={"status": "running"})
        assert res.status_code == 404

    def test_job_files_post(
        self,
        env: str,
        queue: RDSJobQueue,
        base_filesystem: FileSystem,
        base_job: SubmittedJob,
        test_username: str,
        client: TestClient,
    ) -> None:
        queue.enqueue(base_job)
        res = client.get(self.endpoint, params={"memory": 1})
        res = client.post(
            f"{self.endpoint}/1/files/url",
            params={"type": "output", "base_path": "test"},
        )
        assert res.status_code == 201
        if env == "local":
            req_base = client
        else:
            req_base = requests  # type: ignore
        res = req_base.request(
            **res.json(),
            files={
                "file": (
                    "file.txt",
                    BytesIO(bytes("content", "utf-8")),
                    "text/plain",
                ),
            },
        )
        res.raise_for_status()
        if env == "local":
            base_filesystem = cast(LocalFilesystem, base_filesystem)
            assert os.path.exists(
                f"{base_filesystem.base_post_path}/{test_username}/test_out/1/test/file.txt"
            )
        else:
            base_filesystem = cast(S3Filesystem, base_filesystem)
            assert (
                base_filesystem.s3_client.get_object(
                    Bucket=base_filesystem.bucket,
                    Key=f"{test_username}/test_out/1/test/file.txt",
                )["Body"]
                .read()
                .decode("utf-8")
                == "content"
            )
