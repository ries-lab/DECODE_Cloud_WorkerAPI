import os
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import ModelStates, EnvironmentTypes
from src.main import app
from src.database import get_db, Base
from src.config import get_settings
from src.queue import get_queues
from src.core.queue import get_queue
from src.crud.model import get_model, update_model_state


# Override settings
settings_path = os.path.join(os.path.dirname(__file__), ".env.test")
settings = get_settings(settings_path)

# Override queue
def override_get_queues():
    queues = {env.value: get_queue(getattr(settings, f"{env.name}_queue".upper()), create_if_not_exists=True)
        for env in EnvironmentTypes}
    return queues
app.dependency_overrides[get_queues] = override_get_queues

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
def any_queue():
    queues = override_get_queues()
    yield queues['any']
    for queue in queues.values():
        queue.delete()  # cleanup

@pytest.fixture
def local_queue():
    queues = override_get_queues()
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
