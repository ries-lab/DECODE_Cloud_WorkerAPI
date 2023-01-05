import os
import pytest
from dotenv import dotenv_values
from fastapi.testclient import TestClient
from src.models import ModelStates
from src.main import app
from src.config import get_settings
from src.core.queue import get_queue


client = TestClient(app)

settings_path = os.path.join(os.path.dirname(__file__), ".env.test")

def get_settings_override():
    return get_settings(settings_path)

app.dependency_overrides[get_settings] = get_settings_override


@pytest.fixture
def queue():
    queue_path = dotenv_values(settings_path)["QUEUE_PATH"]
    yield get_queue(queue_path)
    os.remove(queue_path)  # cleanup


def test_train(queue):
    response = client.post("/models", json={"name": "test_train"})
    model_id = response.json()["id"]
    response = client.post("/train", json={
        "model_id": model_id,
        "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
    # test enqueued
    print(queue.peek()[0])
    assert queue.peek()[0]["model_id"] == model_id
    # test model status
    assert client.get(f"/models/{model_id}").json()["status"] == ModelStates.training.value
