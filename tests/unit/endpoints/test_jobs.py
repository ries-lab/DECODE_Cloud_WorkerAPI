import pytest
import time
from fastapi.testclient import TestClient
from tests.conftest import monkeypatch_module, base_filesystem
from tests.unit.core.test_queue import jobs, full_jobs, populated_queue, populated_full_queue
from workerfacing_api.core import queue as core_queue
from workerfacing_api.dependencies import get_queue
from workerfacing_api.main import workerfacing_app


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


@pytest.fixture(autouse=True)
def env_name():
    return "local"


def test_get_jobs_required_params(populated_full_queue, env_name):
    required = ["hostname", "cpu_cores", "memory", "environment"]
    base_query_params = {"hostname": "i", "cpu_cores": 2, "memory": 1, "environment": env_name}
    for param in required:
        query_params = base_query_params.copy()
        del query_params[param]
        res = client.get(endpoint, params=query_params)
        assert res.status_code == 422
        assert res.json()["detail"][0]["type"] == "value_error.missing"

def test_get_jobs_filtering_cpu_cores(populated_full_queue, env_name):
    res = client.get(endpoint, params={"hostname": "i", "cpu_cores": 2, "memory": 1, "environment": env_name})
    assert list(res.json().values())[0]["meta"]["job_id"] == 1

def test_get_jobs_filtering_memory(populated_full_queue, env_name):
    res = client.get(endpoint, params={"hostname": "i", "cpu_cores": 999, "memory": 1, "environment": env_name})
    print(res.json())
    assert list(res.json().values())[0]["meta"]["job_id"] == 1

def test_get_jobs_filtering_gpu_model(populated_full_queue, env_name):
    res = client.get(endpoint, params={"hostname": "i", "cpu_cores": 999, "memory": 999, "gpu_model": "gpu_model", "environment": env_name})
    assert list(res.json().values())[0]["meta"]["job_id"] == 1

def test_get_jobs_filtering_gpu_archi(populated_full_queue, env_name):
    res = client.get(endpoint, params={"hostname": "i", "cpu_cores": 999, "memory": 999, "gpu_archi": "gpu_archi", "environment": env_name})
    assert list(res.json().values())[0]["meta"]["job_id"] == 1

def test_get_jobs_priorities(populated_full_queue, env_name):
    # group priority
    res = client.get(endpoint, params={"hostname": "i", "cpu_cores": 999, "memory": 999, "groups": ["group", "another group"], "environment": env_name})
    assert list(res.json().values())[0]["meta"]["job_id"] == 2
    # job priority
    res = client.get(endpoint, params={"hostname": "i", "cpu_cores": 999, "memory": 999, "environment": env_name})
    assert list(res.json().values())[0]["meta"]["job_id"] == 1
    res = client.get(endpoint, params={"hostname": "i", "cpu_cores": 999, "memory": 999, "environment": env_name})
    assert list(res.json().values())[0]["meta"]["job_id"] == 3

def test_get_jobs_dequeue_old(populated_queue, env_name):
    # older_than does not apply when the right environment is selected
    # not old enough
    res = client.get(endpoint, params={"hostname": "i", "cpu_cores": 999, "memory": 999, "environment": env_name, "older_than": 5})
    assert len(res.json()) == 1
    res = client.get(endpoint, params={"hostname": "i", "cpu_cores": 999, "memory": 999, "environment": env_name, "older_than": 5})
    assert len(res.json()) == 1
    res = client.get(endpoint, params={"hostname": "i","cpu_cores": 999, "memory": 999,  "environment": env_name, "older_than": 5})
    assert len(res.json()) == 0
    # old enough
    time.sleep(5)
    res = client.get(endpoint, params={"hostname": "i", "cpu_cores": 999, "memory": 999, "environment": env_name, "older_than": 5})
    assert len(res.json()) == 1
