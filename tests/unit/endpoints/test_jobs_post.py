import datetime
import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock
from tests.conftest import monkeypatch_module, internal_api_key_secret, patch_update_job
from workerfacing_api.dependencies import get_queue
from workerfacing_api.main import workerfacing_app


client = TestClient(workerfacing_app)
endpoint = "/_jobs"


@pytest.fixture(scope="function")
def queue_enqueue(monkeypatch_module):
    queue = MagicMock()
    queue.enqueue = MagicMock()
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,
        get_queue,
        lambda: queue,
    )
    return queue.enqueue


@pytest.fixture(scope="function")
def queue_job():
    return {
        "job": {
            "app": {},
            "handler": {"image_url": "a"},
            "meta": {"job_id": 1, "date_created": datetime.datetime.now().isoformat()},
        },
        "hardware": {},
        "environment": "cloud",
        "paths_upload": {"output": "out", "log": "log"},
    }


def test_post_job(queue_enqueue, queue_job, patch_update_job):
    resp = client.post(endpoint, headers={"x-api-key": internal_api_key_secret}, json=queue_job)
    assert resp.json()["job"]["meta"]["job_id"] == 1
    queue_enqueue.assert_called_once()
