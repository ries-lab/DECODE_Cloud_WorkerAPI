import datetime
import pytest
import time
from workerfacing_api.core.queue import LocalJobQueue, SQSJobQueue, RDSJobQueue
from api.models import JobStates, JobTypes


@pytest.fixture
def jobs():
    time_now = datetime.datetime.utcnow()
    job1 = {
        "job_type": JobTypes.train.value,
        "date_created": str(time_now - datetime.timedelta(seconds=10 * 60)),
        "status": JobStates.pending.value,
        "model_id": 0,
    }
    job2 = {
        "job_type": JobTypes.train.value,
        "date_created": str(time_now),
        "status": JobStates.pending.value,
        "model_id": 1,
    }
    return job1, job2


@pytest.fixture
def populated_queue(queue, jobs, env_name):
    job1, job2 = jobs
    queue.enqueue(env=env_name, item=job1)
    queue.enqueue(env=env_name, item=job2)
    return queue


class TestLocalQueue:

    @pytest.fixture
    def queue(self, tmpdir):
        queue_path = str(tmpdir / 'queue.pkl')
        job_queue = LocalJobQueue(queue_path)
        job_queue.create()
        yield job_queue
        job_queue.delete()
        
    @pytest.fixture
    def env_name(self):
        return 'test_queue'

    def test_create_queue(self, queue, env_name):
        # test queue is empty
        assert queue.peek(env=env_name)[0] is None

    def test_enqueue(self, populated_queue, env_name):
        assert populated_queue.peek(env=env_name)[0] is not None

    def test_peek(self, populated_queue, env_name):
        assert populated_queue.peek(env=env_name)[0]['model_id'] == 0
        # peeking does not remove elements
        assert populated_queue.peek(env=env_name)[0]['model_id'] == 0
    
    def test_dequeue(self, populated_queue, env_name):
        assert populated_queue.dequeue(env=env_name)['model_id'] == 0
        # dequeue removes elements
        assert populated_queue.peek(env=env_name)[0]['model_id'] == 1
    
    def test_dequeue_old(self, populated_queue, env_name):
        # old enough
        assert populated_queue.dequeue(env=env_name, older_than=5*60) is not None
        # not old enough
        assert populated_queue.dequeue(env=env_name, older_than=5*60) is None


@pytest.mark.skip("too slow in development")
class TestsSQSQueue(TestLocalQueue):
    i = 0

    @pytest.fixture
    def env_name(self):
        TestsSQSQueue.i = TestsSQSQueue.i + 1
        queue_name = f"decode_test_sqs_queue_{self.i}"
        return queue_name
    
    @pytest.fixture
    def queue(self, env_name):
        # need new env name for each test (SQS queues can't be recreated after less than 60 seconds)
        job_queue = SQSJobQueue([env_name])
        job_queue.create(err_on_exists=False)
        yield job_queue
        job_queue.delete()


class TestRDSQueue(TestLocalQueue):
    
    @pytest.fixture
    def queue(self, tmpdir, env_name):
        job_queue = RDSJobQueue(f"sqlite:///{tmpdir}/{env_name}.db")
        job_queue.create()
        yield job_queue
        job_queue.delete()
    
    def test_dequeue_old(self, populated_queue, env_name):
        # old enough
        assert populated_queue.dequeue(env=env_name, older_than=0.1) is not None
        # not old enough
        assert populated_queue.dequeue(env=env_name, older_than=5*60) is None
