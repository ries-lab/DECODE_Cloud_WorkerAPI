import os
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import ModelStates
from src.main import app
from src.database import get_db, Base
from src.config import get_settings
from src.queue import get_queue
from src.core.queue import get_queue_from_path
from src.crud.model import get_model, update_model_state


# Override settings
settings_path = os.path.join(os.path.dirname(__file__), ".env.test")
settings = get_settings(settings_path)

# Override queue
def override_get_queue():
    return get_queue_from_path(settings.QUEUE_PATH)
app.dependency_overrides[get_queue] = override_get_queue

# Override DB
SQLALCHEMY_DATABASE_URL = settings.DATABASE_URL
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)

def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

# Client
client = TestClient(app)


@pytest.fixture(scope="module")
def db():
    return next(override_get_db())

@pytest.fixture
def queue():
    job_queue = get_queue_from_path(settings.QUEUE_PATH, create_if_not_exists=True)
    yield job_queue
    job_queue.delete()  # cleanup

@pytest.fixture
def model_untrained():
    response = client.post("/models", json={"name": "test_untrained"})
    model_id = response.json()["id"]
    return model_id

@pytest.fixture
def model_trained(db):
    response = client.post("/models", json={"name": "test_trained"})
    model_id = response.json()["id"]
    update_model_state(db, get_model(db, model_id), ModelStates.trained).status
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

    def test_train(self, queue, model_untrained):
        client.post("/train", json={
            "model_id": model_untrained,
            "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}}).json()
        # test enqueued
        assert queue.peek()[0]["model_id"] == model_untrained
        # test model status
        assert client.get(f"/models/{model_untrained}").json()["status"] == ModelStates.training.value
    
    def test_train_model_trained(self, queue, model_trained):
        resp = client.post("/train", json={
            "model_id": model_trained,
            "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
        # test not enqueued
        peek = queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_train_model_training(self, queue, model_training):
        resp = client.post("/train", json={
            "model_id": model_training,
            "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
        # test not enqueued
        peek = queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_train_model_unexistent(self, queue, model_unexistent):
        resp = client.post("/train", json={
            "model_id": model_unexistent,
            "attributes": {"config_file": "", "model_file": "", "inference_config_file": ""}})
        # test not enqueued
        peek = queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 404


class TestPredict:

    def test_predict(self, queue, model_trained):
        print(client.post("/predict", json={
            "model_id": model_trained,
            "attributes": {"data_file": ""}}))
        # test enqueued
        assert queue.peek()[0]["model_id"] == model_trained

    def test_predict_model_untrained(self, queue, model_untrained):
        resp = client.post("/predict", json={
                "model_id": model_untrained,
                "attributes": {"data_file": ""}})
        # test not enqueued
        peek = queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_predict_model_training(self, queue, model_training):
        resp = client.post("/predict", json={
                "model_id": model_training,
                "attributes": {"data_file": ""}})
        # test not enqueued
        peek = queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 400

    def test_predict_model_unexistent(self, queue, model_unexistent):
        resp = client.post("/predict", json={
                "model_id": model_unexistent,
                "attributes": {"data_file": ""}})
        # test not enqueued
        peek = queue.peek()[0]
        assert not peek or peek["model_id"] != model_trained
        # test error code
        assert resp.status_code == 404
