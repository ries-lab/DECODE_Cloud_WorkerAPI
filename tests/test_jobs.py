import os
import pytest
from dotenv import dotenv_values
from fastapi import HTTPException
from fastapi.testclient import TestClient
from src.models import ModelStates
from src.main import app
from src.config import get_settings
from src.core.queue import get_queue
from src.crud.model import update_model_state


client = TestClient(app)
settings_path = os.path.join(os.path.dirname(__file__), ".env.test")

def get_settings_override():
    return get_settings(settings_path)

app.dependency_overrides[get_settings] = get_settings_override


@pytest.fixture
def queue():
    queue_path = dotenv_values(settings_path)["QUEUE_PATH"]
    queue = get_queue(queue_path, create_if_not_exists=True)
    yield queue
    queue.delete()  # cleanup

@pytest.fixture(scope="module")
def model_untrained():
    response = client.post("/models", json={"name": "test_untrained"})
    model_id = response.json()["id"]
    return model_id

@pytest.fixture(scope="module")
def model_trained():
    response = client.post("/models", json={"name": "test_trained"})
    model_id = response.json()["id"]
    client.patch(f"/models/{model_id}", json={"status": ModelStates.trained.value})
    return model_id

@pytest.fixture(scope="module")
def model_training():
    response = client.post("/models", json={"name": "test_training"})
    model_id = response.json()["id"]
    client.patch(f"/models/{model_id}", json={"status": ModelStates.training.value})
    return model_id

@pytest.fixture(scope="module")
def model_unexistent():
    response = client.get("/models")
    model_id = max([m["id"] for m in response.json()]) + 1
    return model_id


class TestTrain:

    def test_train(self, queue, model_untrained):
        client.post("/train", json={
            "model_id": model_untrained,
            "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
        # test enqueued
        assert queue.peek()[0]["model_id"] == model_untrained
        # test model status
        assert client.get(f"/models/{model_untrained}").json()["status"] == ModelStates.training.value
    
    def test_train_model_trained(self, queue, model_trained):
        with pytest.raises(HTTPException) as err:
            client.post("/train", json={
                "model_id": model_trained,
                "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
        assert err.value.status_code == 400

    def test_train_model_training(self, queue, model_training):
        with pytest.raises(HTTPException) as err:
            client.post("/train", json={
                "model_id": model_training,
                "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
        assert err.value.status_code == 400

    def test_train_model_unexistent(self, queue, model_unexistent):
        with pytest.raises(HTTPException) as err:
            client.post("/train", json={
                "model_id": model_unexistent,
                "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
        assert err.value.status_code == 404


class TestPredict:

    def test_predict(self, queue, model_trained):
        print(client.post("/predict", json={
            "model_id": model_trained,
            "attributes": {"data_file": ""}}))
        # test enqueued
        assert queue.peek()[0]["model_id"] == model_trained

    def test_predict_model_untrained(self, queue, model_untrained):
        with pytest.raises(HTTPException) as err:
            client.post("/predict", json={
                "model_id": model_untrained,
                "attributes": {"data_file": ""}})
        assert err.value.status_code == 404

    def test_predict_model_training(self, queue, model_training):
        with pytest.raises(HTTPException) as err:
            client.post("/predict", json={
                "model_id": model_training,
                "attributes": {"data_file": ""}})
        assert err.value.status_code == 404

    def test_predict_model_unexistent(self, queue, model_unexistent):
        with pytest.raises(HTTPException) as err:
            client.post("/predict", json={
                "model_id": model_unexistent,
                "attributes": {"data_file": ""}})
        assert err.value.status_code == 404
