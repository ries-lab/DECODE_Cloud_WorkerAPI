import time
from io import BytesIO

import pytest
from fastapi.testclient import TestClient

from workerfacing_api.core import queue as core_queue
from workerfacing_api.dependencies import get_queue
from workerfacing_api.main import workerfacing_app
from workerfacing_api.schemas.rds_models import JobStates

client = TestClient(workerfacing_app)
endpoint = "/jobs"


@pytest.fixture(autouse=True)
def queue(monkeypatch_module, tmpdir):
    queue_ = core_queue.RDSJobQueue(f"sqlite:///{tmpdir}/test_queue.db")
    queue_.create(err_on_exists=False)
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides, get_queue, lambda: queue_
    )
    yield queue_
    queue_.delete()


def test_get_jobs(populated_full_queue, patch_update_job):
    resp = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999},
    )
    assert resp.status_code == 200
    patch_update_job.assert_called_once_with(1, JobStates.pulled, None)


def test_get_jobs_required_params(populated_full_queue):
    required = ["memory"]
    base_query_params = {"memory": 1}
    for param in required:
        query_params = base_query_params.copy()
        del query_params[param]
        res = client.get(endpoint, params=query_params)
        assert res.status_code == 422
        assert "missing" in res.json()["detail"][0]["type"]


def test_get_jobs_filtering_cpu_cores(populated_full_queue):
    res = client.get(
        endpoint,
        params={"cpu_cores": 2, "memory": 1},
    )
    assert list(res.json().values())[0]["meta"]["job_id"] == 1


def test_get_jobs_filtering_memory(populated_full_queue):
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 1},
    )
    assert list(res.json().values())[0]["meta"]["job_id"] == 1


def test_get_jobs_filtering_gpu_model(populated_full_queue):
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999, "gpu_model": "gpu_model"},
    )
    assert list(res.json().values())[0]["meta"]["job_id"] == 1


def test_get_jobs_filtering_gpu_archi(populated_full_queue):
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999, "gpu_archi": "gpu_archi"},
    )
    assert list(res.json().values())[0]["meta"]["job_id"] == 1


def test_get_jobs_priorities(populated_full_queue):
    # group priority
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999, "groups": ["group", "another group"]},
    )
    assert list(res.json().values())[0]["meta"]["job_id"] == 2
    # job priority
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999},
    )
    assert list(res.json().values())[0]["meta"]["job_id"] == 1
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999},
    )
    assert list(res.json().values())[0]["meta"]["job_id"] == 3


def test_get_jobs_dequeue_old(populated_queue):
    # older_than does not apply when the right environment is selected
    # not old enough
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999, "older_than": 5},
    )
    assert len(res.json()) == 1
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999, "older_than": 5},
    )
    assert len(res.json()) == 1
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999, "older_than": 5},
    )
    assert len(res.json()) == 0
    # old enough
    time.sleep(5)
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999, "older_than": 5},
    )
    assert len(res.json()) == 1


def test_get_job_status(populated_full_queue):
    res = client.get(f"{endpoint}/1/status")
    assert res.json() == "queued"
    # dequeue
    res = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999},
    )
    res = client.get(f"{endpoint}/{list(res.json().keys())[0]}/status")
    assert res.json() == "pulled"


def test_put_job_status(populated_full_queue):
    # job needs to be pulled in order to update its status
    res = client.put(f"{endpoint}/1/status", params={"status": "running"})
    assert str(res.status_code).startswith("4")
    resp = client.get(endpoint, params={"cpu_cores": 999, "memory": 999})
    job_id = list(resp.json().keys())[0]
    res = client.put(f"{endpoint}/{job_id}/status", params={"status": "running"})
    assert res.status_code == 200
    res = client.get(f"{endpoint}/{job_id}/status")
    assert res.json() == "running"


def test_job_files_post(full_jobs, populated_full_queue, base_filesystem):
    file_name = "test_file.txt"
    content = "test content"
    job = full_jobs[0]
    job_dequeue = client.get(
        endpoint,
        params={"cpu_cores": 999, "memory": 999},
    ).json()
    job_id, job_dequeue = list(job_dequeue.items())[0]
    res = client.post(
        f"{endpoint}/{job_id}/files/upload",
        params={"type": "output", "path": file_name},
        files={
            "file": (
                file_name,
                BytesIO(bytes(content, "utf-8")),
                "text/plain",
            ),
        },
    )
    assert res.status_code == 201
    path = f"{job.paths_upload.output}/{file_name}"
    res = client.get(f"/files/{path}/url")
    assert res.status_code == 200
