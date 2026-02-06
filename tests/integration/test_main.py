import gzip
import sqlite3
import tempfile
import time
from typing import cast

import pytest
from fastapi.testclient import TestClient

from tests.conftest import S3TestingBucket
from workerfacing_api import settings
from workerfacing_api.core.filesystem import FileSystem, S3Filesystem
from workerfacing_api.core.queue import RDSJobQueue, SQLiteRDSJobQueue
from workerfacing_api.dependencies import queue_dep
from workerfacing_api.main import workerfacing_app
from workerfacing_api.schemas.queue_jobs import SubmittedJob
from workerfacing_api.schemas.rds_models import JobStates


@pytest.fixture
def client() -> TestClient:
    return TestClient(workerfacing_app)


class TestCronHandleTimeouts:
    @pytest.fixture(autouse=True)
    def setup_timeout_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Set timeout_failure to 2 seconds for faster testing."""
        monkeypatch.setattr(settings, "timeout_failure", 2)

    @pytest.fixture(autouse=True)
    def setup_max_retries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Set max retries to 1 for faster testing."""
        monkeypatch.setattr(settings, "max_retries", 1)

    @pytest.fixture(autouse=True)
    def setup_cron_interval(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Set cron interval to 1 second for faster testing."""
        monkeypatch.setattr(settings, "cron_timeout_check_interval", 1)

    def test_handle_timeouts(
        self,
        queue: RDSJobQueue,
        base_job: SubmittedJob,
        client: TestClient,
    ) -> None:
        job_id = base_job.job.meta.job_id
        with client:
            # Push the job
            queue.enqueue(base_job)
            job = queue.get_job(job_id)
            assert job.status == JobStates.queued.value
            assert job.num_retries == 0

            # Pull the job
            get_params = {"memory": 1}
            assert len(client.get("/jobs", params=get_params).json()) == 1
            job = queue.get_job(job_id)
            assert job.status == JobStates.pulled.value
            assert job.num_retries == 0

            # Job kept alive by periodic status updates
            for _ in range(4):
                time.sleep(1)
                client.put(
                    f"/jobs/{job_id}/status",
                    params={"status": "running", "runtime_details": "Processing..."},
                )
                assert len(client.get("/jobs", params=get_params).json()) == 0
                job = queue.get_job(job_id)
                assert job.status == JobStates.running.value
                assert job.num_retries == 0

            # Let timeout
            time.sleep(4)
            job = queue.get_job(job_id)
            assert job.status == JobStates.queued.value
            assert job.num_retries == 1

            # Pull again
            assert len(client.get("/jobs", params=get_params).json()) == 1
            job = queue.get_job(job_id)
            assert job.status == JobStates.pulled.value
            assert job.num_retries == 1

            # Let timeout and fail
            time.sleep(4)
            job = queue.get_job(job_id)
            assert job.status == JobStates.error.value
            assert job.num_retries == 1


class TestCronBackupDatabase:
    @pytest.fixture(autouse=True)
    def skip_if_not_sqlite_s3(
        self, queue: RDSJobQueue, base_filesystem: FileSystem
    ) -> None:
        """Skip tests if not using SQLite queue with S3 filesystem."""
        if not isinstance(queue, SQLiteRDSJobQueue) or not isinstance(
            base_filesystem, S3Filesystem
        ):
            pytest.skip("Backup tests only run with SQLite queue and S3 filesystem")

    @pytest.fixture(autouse=True)
    def setup_backup_cron_interval(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Set backup cron interval to 1 seconds for faster testing."""
        monkeypatch.setattr(settings, "cron_backup_interval", 1)

    def get_backup_nrows(self, s3_testing_bucket: S3TestingBucket) -> int:
        """Helper to get number of rows in backup database."""
        response = s3_testing_bucket.s3_client.get_object(
            Bucket=s3_testing_bucket.bucket_name,
            Key=SQLiteRDSJobQueue.BACKUP_KEY,
        )
        backup_data_gzip = response["Body"].read()
        backup_data = gzip.decompress(backup_data_gzip)
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp_file:
            tmp_file.write(backup_data)
            tmp_path = tmp_file.name
            conn = sqlite3.connect(tmp_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM queued_jobs")
            n_rows = cursor.fetchone()[0]
            conn.close()
        return cast(int, n_rows)

    def test_sqlite_backup(
        self,
        queue: SQLiteRDSJobQueue,
        base_job: SubmittedJob,
        client: TestClient,
        s3_testing_bucket: S3TestingBucket,
        tmpdir_factory: pytest.TempdirFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test the backup and restore functionality of the SQLiteRDSJobQueue."""
        # Startup: no backup present
        with pytest.raises(s3_testing_bucket.s3_client.exceptions.NoSuchKey):
            self.get_backup_nrows(s3_testing_bucket)

        with client:
            # First start-up: no jobs
            time.sleep(2)  # wait for backup to run
            assert self.get_backup_nrows(s3_testing_bucket) == 0

            # Enqueue a job and verify it's backed up
            queue.enqueue(base_job)
            time.sleep(2)  # wait for backup to run
            assert self.get_backup_nrows(s3_testing_bucket) == 1

            # Enqueue a second job and shutdown before backup runs
            queue.enqueue(base_job)

        # On shutdown, final backup should run with both jobs
        assert self.get_backup_nrows(s3_testing_bucket) == 2

        # New queue (e.g., application started again) should restore from backup
        new_db_url = f"sqlite:///{tmpdir_factory.mktemp('integration') / 'restored.db'}"
        new_queue = SQLiteRDSJobQueue(
            new_db_url,
            s3_client=s3_testing_bucket.s3_client,
            s3_bucket=s3_testing_bucket.bucket_name,
        )
        monkeypatch.setitem(
            workerfacing_app.dependency_overrides,  # type: ignore
            queue_dep,
            lambda: new_queue,
        )
        with client:
            assert (
                len(client.get("/jobs", params={"memory": 1, "limit": 5}).json()) == 2
            )
            assert self.get_backup_nrows(s3_testing_bucket) == 2
