import pytest

from fastapi.testclient import TestClient
from api.models import ModelStates
from api.main import app
from api.queue import get_queues
from api.crud.model import get_model, update_model_state
import api.settings as settings

testing_local_queue = "testing_local"
testing_cloud_queue = "testing_cloud"
testing_any_queue = "testing_any"


@pytest.fixture(autouse=True)
def override_get_queues(monkeypatch):
    monkeypatch.setattr(settings, "local_queue", testing_local_queue)
    monkeypatch.setattr(settings, "cloud_queue", testing_cloud_queue)
    monkeypatch.setattr(settings, "any_queue", testing_any_queue)


# Client
client = TestClient(app)


@pytest.fixture
def any_queue():
    queues = get_queues()
    yield queues['any']
    for queue in queues.values():
        queue.delete()  # cleanup


@pytest.fixture
def local_queue():
    queues = get_queues()
    yield queues['local']
    for queue in queues.values():
        queue.delete()  # cleanup


@pytest.fixture
def model_untrained():
    response = client.post("/models", json={"name": "test_untrained"})
    model_id = response.json()["id"]
    return model_id


@pytest.fixture
def model_trained(db):
    response = client.post("/models", json={"name": "test_trained"})
    model_id = response.json()["id"]
    update_model_state(db, get_model(db, model_id), ModelStates.trained)
    return model_id


@pytest.fixture
def model_training(db):
    response = client.post("/models", json={"name": "test_training"})
    model_id = response.json()["id"]
    update_model_state(db, get_model(db, model_id), ModelStates.training)
    return model_id


@pytest.fixture
def model_unexistent():
    response = client.get("/models")
    model_id = max([m["id"] for m in response.json()]) + 1
    return model_id


class TestTrain:

    def test_train(self, any_queue, local_queue, model_untrained):
        client.post("/train", json={
            "model_id": model_untrained,
            "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}}).json()
        # test enqueued
        assert any_queue.peek()[0]["model_id"] == model_untrained
        # test not enqueued in other queue
        peek = local_queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test model status
        assert client.get(f"/models/{model_untrained}").json()["status"] == ModelStates.training.value
    
    def test_train_model_trained(self, any_queue, model_trained):
        resp = client.post("/train", json={
            "model_id": model_trained,
            "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
        # test not enqueued
        peek = any_queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_train_model_training(self, any_queue, model_training):
        resp = client.post("/train", json={
            "model_id": model_training,
            "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
        # test not enqueued
        peek = any_queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_train_model_unexistent(self, any_queue, model_unexistent):
        resp = client.post("/train", json={
            "model_id": model_unexistent,
            "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
        # test not enqueued
        peek = any_queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 404


class TestPredict:

    def test_predict(self, any_queue, model_trained):
        client.post("/predict", json={
            "model_id": model_trained,
            "attributes": {"data_file": ""}})
        # test enqueued
        assert any_queue.peek()[0]["model_id"] == model_trained

    def test_predict_model_untrained(self, any_queue, model_untrained):
        resp = client.post("/predict", json={
                "model_id": model_untrained,
                "attributes": {"data_file": ""}})
        # test not enqueued
        peek = any_queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_predict_model_training(self, any_queue, model_training):
        resp = client.post("/predict", json={
                "model_id": model_training,
                "attributes": {"data_file": ""}})
        # test not enqueued
        peek = any_queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_predict_model_unexistent(self, any_queue, model_unexistent):
        resp = client.post("/predict", json={
                "model_id": model_unexistent,
                "attributes": {"data_file": ""}})
        # test not enqueued
        peek = any_queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 404
