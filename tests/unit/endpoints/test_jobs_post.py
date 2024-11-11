import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from workerfacing_api.dependencies import get_queue
from workerfacing_api.main import workerfacing_app

client = TestClient(workerfacing_app)
endpoint = "/_jobs"


@pytest.fixture(scope="function")
def queue_enqueue(
    monkeypatch_module: pytest.MonkeyPatch,
) -> MagicMock:
    queue = MagicMock()
    queue.enqueue = MagicMock()
    monkeypatch_module.setitem(
        workerfacing_app.dependency_overrides,  # type: ignore
        get_queue,
        lambda: queue,
    )
    return queue.enqueue


@pytest.fixture(scope="function")
def queue_job() -> dict[str, Any]:
    return {
        "job": {
            "app": {},
            "handler": {"image_url": "a"},
            "meta": {"job_id": 1, "date_created": datetime.datetime.now().isoformat()},
            "hardware": {},
        },
        "environment": "cloud",
        "paths_upload": {"output": "out", "log": "log", "artifact": "model"},
    }


def test_post_job(
    queue_enqueue: MagicMock,
    queue_job: dict[str, Any],
    patch_update_job: MagicMock,
    internal_api_key_secret: str,
) -> None:
    resp = client.post(
        endpoint, headers={"x-api-key": internal_api_key_secret}, json=queue_job
    )
    assert resp.json()["job"]["meta"]["job_id"] == 1
    queue_enqueue.assert_called_once()
