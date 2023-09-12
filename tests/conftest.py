import pytest
import dotenv
dotenv.load_dotenv()

from io import BytesIO
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from api.main import app
from api.dependencies import current_user_dep, CognitoClaims
from api.database import get_db, Base

root_file1_name = 'dfile.txt'
root_file2_name = 'cfile.txt'
subdir_name = 'test_dir/'
subdir_file1_name = 'test_dir/test_file.txt'
root_file1_contents = 'file contents'
root_file2_contents = 'file2 contents'
subdir_file1_contents = 'subdir file contents'

test_username = "test_user"

testing_database = "sqlite:///./test.db"


@pytest.fixture(scope="session")
def monkeypatch_session():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(autouse=True, scope="session")
def override_auth(monkeypatch_session):
    monkeypatch_session.setitem(
        app.dependency_overrides,
        current_user_dep,
        lambda: CognitoClaims(**{"cognito:username": test_username, "email": "test@example.com"}),
    )


@pytest.fixture
def require_auth(monkeypatch):
    monkeypatch.delitem(app.dependency_overrides, current_user_dep)


@pytest.fixture(scope="session")
def db():
    # Override DB
    engine = create_engine(
        testing_database, connect_args={"check_same_thread": False}
    )
    make_testing_session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    db = make_testing_session()
    yield db
    db.close()
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(autouse=True, scope="session")
def override_get_db(db, monkeypatch_session):
    monkeypatch_session.setitem(app.dependency_overrides, get_db, lambda: db)


@pytest.fixture
def multiple_files(filesystem):
    filesystem.create_file(root_file1_name, BytesIO(bytes(root_file1_contents, 'utf-8')))
    filesystem.create_file(root_file2_name, BytesIO(bytes(root_file2_contents, 'utf-8')))
    filesystem.create_file(subdir_file1_name, BytesIO(bytes(subdir_file1_contents, 'utf-8')))
    yield
    filesystem.delete(root_file1_name)
    filesystem.delete(root_file2_name)
    filesystem.delete(subdir_file1_name)


@pytest.fixture
def single_file(filesystem):
    filesystem.create_file(root_file1_name, BytesIO(bytes(root_file1_contents, 'utf-8')))
    yield
    filesystem.delete(root_file1_name)


@pytest.fixture
def cleanup_files(filesystem):
    to_cleanup = []
    yield to_cleanup
    for file in to_cleanup:
        filesystem.delete(file)
