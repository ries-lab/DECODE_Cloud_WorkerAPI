import datetime
import pytest
from src.core.queue import LocalJobQueue
from src.models import Job, JobStates, JobTypes


class TestQueue:
    @pytest.fixture
    def create_jobs(self):
        time_now = datetime.datetime.utcnow()
        job1 = Job(
            job_type = JobTypes.train,
            date_created = time_now - datetime.timedelta(seconds=10*60),
            status = JobStates.pending,
            model_id = 0,
        )
        job2 = Job(
            job_type = JobTypes.train,
            date_created = time_now,
            status = JobStates.pending,
            model_id = 1,
        )
        return (job1, job2)
    
    @pytest.fixture
    def queue_path(self, tmpdir):
        path_ = tmpdir / 'queue.pkl'
        return str(path_)
    
    @pytest.fixture
    def create_queue(self, queue_path):
        job_queue = LocalJobQueue(queue_path)
        return job_queue

    def test_create_queue(self, create_queue):
        job_queue = create_queue
        # test queue is empty
        assert job_queue.peek()[0] is None
    
    @pytest.fixture
    def create_populated_queue(self, create_queue, create_jobs):
        job_queue = create_queue
        job1, job2 = create_jobs
        job_queue.enqueue(job1)
        job_queue.enqueue(job2)
        return job_queue

    def test_enqueue(self, create_populated_queue):
        job_queue = create_populated_queue
        assert job_queue.peek()[0] is not None

    def test_peek(self, create_populated_queue):
        job_queue = create_populated_queue
        assert job_queue.peek()[0]['model_id'] == 0
        # peeking does not remove elements
        assert job_queue.peek()[0]['model_id'] == 0
    
    def test_dequeue(self, create_populated_queue):
        job_queue = create_populated_queue
        assert job_queue.dequeue().model_id == 0
        # dequeue removes elements
        assert job_queue.peek()[0]['model_id'] == 1
    
    def test_dequeue_old(self, create_populated_queue):
        job_queue = create_populated_queue
        # old enough
        assert job_queue.dequeue(older_than=5*60) is not None
        # not old enough
        assert job_queue.dequeue(older_than=5*60) is None

