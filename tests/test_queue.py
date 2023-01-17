import datetime
import pytest
import time
from api.core.queue import LocalJobQueue, SQSJobQueue
from api.models import Job, JobStates, JobTypes


class TestLocalQueue:
    @pytest.fixture
    def jobs(self):
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
    def queue(self, queue_path):
        job_queue = LocalJobQueue(queue_path)
        job_queue.create()
        yield job_queue
        job_queue.delete()

    def test_create_queue(self, queue):
        # test queue is empty
        assert queue.peek()[0] is None
    
    @pytest.fixture
    def populated_queue(self, queue, jobs):
        job1, job2 = jobs
        queue.enqueue(job1)
        queue.enqueue(job2)
        return queue

    def test_enqueue(self, populated_queue):
        assert populated_queue.peek()[0] is not None

    def test_peek(self, populated_queue):
        assert populated_queue.peek()[0]['model_id'] == 0
        # peeking does not remove elements
        assert populated_queue.peek()[0]['model_id'] == 0
    
    def test_dequeue(self, populated_queue):
        assert populated_queue.dequeue().model_id == 0
        # dequeue removes elements
        assert populated_queue.peek()[0]['model_id'] == 1
    
    def test_dequeue_old(self, populated_queue):
        # old enough
        assert populated_queue.dequeue(older_than=5*60) is not None
        # not old enough
        assert populated_queue.dequeue(older_than=5*60) is None


@pytest.mark.aws
class TestsSQSQueue:
    i = 0
    @pytest.fixture
    def jobs(self):
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
    def queue(self):
        # need new queue name for each test
        TestsSQSQueue.i = TestsSQSQueue.i + 1
        queue_name = f"decode_test_sqs_queue_{self.i}"
        job_queue = SQSJobQueue(queue_name)
        assert not job_queue.exists()
        job_queue.create()
        yield job_queue
        job_queue.delete()

    def test_create_queue(self, queue):
        # test queue is empty
        assert queue.peek()[0] is None
    
    @pytest.fixture
    def populated_queue(self, queue, jobs):
        job1, job2 = jobs
        queue.enqueue(job1)
        queue.enqueue(job2)
        return queue

    def test_enqueue(self, populated_queue):
        assert populated_queue.peek()[0] is not None

    def test_peek(self, populated_queue):
        assert populated_queue.peek()[0]['model_id'] == 0
        time.sleep(6)
        # peeking does not remove elements
        assert populated_queue.peek()[0]['model_id'] == 0
    
    def test_dequeue(self, populated_queue):
        assert populated_queue.dequeue().model_id == 0
        time.sleep(6)
        # dequeue removes elements
        assert populated_queue.peek()[0]['model_id'] == 1
    
    def test_dequeue_old(self, populated_queue):
        # old enough
        assert populated_queue.dequeue(older_than=5*60) is not None
        time.sleep(6)
        # not old enough
        assert populated_queue.dequeue(older_than=5*60) is None
