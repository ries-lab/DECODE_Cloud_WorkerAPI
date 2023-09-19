import pytest
import uvicorn
from fastapi.testclient import TestClient
from multiprocessing import Process

from api.models import ModelStates
from api.main import app
from api.crud.model import get_model, update_model_state
import api.crud.job
import api.settings
from workerfacing_api.core.queue import RDSJobQueue
from workerfacing_api.main import workerfacing_app
from workerfacing_api.queue import get_queue

testing_jobs_db = "sqlite:///./test_jobs.db"
testing_jobs_port = 5000

queue_ = RDSJobQueue(testing_jobs_db)


def override_get_queue():
    return queue_


workerfacing_app.dependency_overrides[get_queue] = override_get_queue


@pytest.fixture
def queue_server(monkeypatch):
    proc = Process(target=lambda: uvicorn.run(workerfacing_app, port=testing_jobs_port), args=(), daemon=True)
    proc.start()
    yield True
    proc.kill()


@pytest.fixture
def queue(monkeypatch, queue_server):
    monkeypatch.setattr(api.settings, "workerfacing_api_url", f"http://127.0.0.1:{testing_jobs_port}")
    queue_.create(err_on_exists=False)
    monkeypatch.setattr(api.crud.job, "_validate_files", lambda x, y: None)  # allow model creation without files
    yield queue_
    queue_.delete()


# Client
client = TestClient(app)


@pytest.fixture
def model_untrained(queue):
    response = client.post("/models", json={"name": "test_untrained", "application": "decode", "version": "v0.10.1"})
    model_id = response.json()["id"]
    return model_id


@pytest.fixture
def model_trained(db, queue):
    response = client.post("/models", json={"name": "test_trained", "application": "decode", "version": "v0.10.1"})
    model_id = response.json()["id"]
    update_model_state(db, get_model(db, model_id), ModelStates.trained)
    return model_id


@pytest.fixture
def model_training(db, queue):
    response = client.post("/models", json={"name": "test_training", "application": "decode", "version": "v0.10.1"})
    model_id = response.json()["id"]
    update_model_state(db, get_model(db, model_id), ModelStates.training)
    return model_id


@pytest.fixture
def model_unexistent(queue):
    response = client.get("/models")
    model_id = max([m["id"] for m in response.json()]) + 1
    return model_id


class TestTrain:

    def test_train(self, queue, model_untrained):
        resp = client.post("/train", json={
            "model_id": model_untrained,
            "environment": "local",
            "attributes": {"model_path": "", "calib_path": "", "param_path": "", "type": "train"}})
        # test enqueued
        assert queue.peek(hostname="i", environment="local")[0]["model_id"] == model_untrained
        # test not enqueued in other queue
        peek = queue.peek(hostname="i", environment="cloud")[0]
        assert not peek or peek["model_id"] != model_trained
        # test model status
        assert client.get(f"/models/{model_untrained}").json()["status"] == ModelStates.training.value
    
    def test_train_model_trained(self, queue, model_trained):
        resp = client.post("/train", json={
            "model_id": model_trained,
            "attributes": {"model_path": "", "calib_path": "", "param_path": "", "type": "train"}})
        # test not enqueued
        peek = queue.peek(hostname="i", environment=None)[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_train_model_training(self, queue, model_training):
        resp = client.post("/train", json={
            "model_id": model_training,
            "attributes": {"model_path": "", "calib_path": "", "param_path": "", "type": "train"}})
        # test not enqueued
        peek = queue.peek(hostname="i", environment=None)[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_train_model_unexistent(self, queue, model_unexistent):
        resp = client.post("/train", json={
            "model_id": model_unexistent,
            "attributes": {"model_path": "", "calib_path": "", "param_path": "", "type": "train"}})
        # test not enqueued
        peek = queue.peek(hostname="i", environment=None)[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 404


class TestPredict:

    def test_predict(self, queue, model_trained):
        resp = client.post("/predict", json={
            "model_id": model_trained,
            "attributes": {"frame_path": "", "frame_meta_path": "", "emitter_path": ""}})
        # test enqueued
        assert queue.peek(hostname="i", environment=None)[0]["model_id"] == model_trained

    def test_predict_model_untrained(self, queue, model_untrained):
        resp = client.post("/predict", json={
                "model_id": model_untrained,
                "attributes": {"frame_path": "", "frame_meta_path": "", "emitter_path": ""}})
        # test not enqueued
        peek = queue.peek(hostname="i", environment=None)[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_predict_model_training(self, queue, model_training):
        resp = client.post("/predict", json={
                "model_id": model_training,
                "attributes": {"frame_path": "", "frame_meta_path": "", "emitter_path": ""}})
        # test not enqueued
        peek = queue.peek(hostname="i", environment=None)[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_predict_model_unexistent(self, queue, model_unexistent):
        resp = client.post("/predict", json={
                "model_id": model_unexistent,
                "attributes": {"frame_path": "", "frame_meta_path": "", "emitter_path": ""}})
        # test not enqueued
        peek = queue.peek(hostname="i", environment=None)[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 404
