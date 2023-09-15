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
    job3 = {
        "job_type": JobTypes.train.value,
        "date_created": str(time_now),
        "status": JobStates.pending.value,
        "model_id": 2,
    }
    job4 = {
        "job_type": JobTypes.train.value,
        "date_created": str(time_now),
        "status": JobStates.pending.value,
        "model_id": 3,
    }
    return job1, job2, job3, job4


@pytest.fixture
def full_jobs():
    time_now = datetime.datetime.utcnow()
    job1 = {
        "job_type": JobTypes.train.value,
        "date_created": str(time_now - datetime.timedelta(seconds=10 * 60)),
        "status": JobStates.pending.value,
        "model_id": 0,
        "cpu_cores": 3,
        "memory": 2,
        "gpu_model": "gpu_model",
        "gpu_archi": "gpu_archi",
        "gpu_mem": 0,
        "group": None,
        "priority": 5,
    }
    job2 = {
        "job_type": JobTypes.train.value,
        "date_created": str(time_now),
        "status": JobStates.pending.value,
        "model_id": 1,
        "cpu_cores": 1,
        "memory": 0,
        "gpu_model": None,
        "gpu_archi": None,
        "gpu_mem": None,
        "group": None,
        "priority": 10,
    }
    job3 = {
        "job_type": JobTypes.train.value,
        "date_created": str(time_now),
        "status": JobStates.pending.value,
        "model_id": 2,
        "group": "group",
        "priority": 1,
    }
    job4 = {
        "job_type": JobTypes.train.value,
        "date_created": str(time_now),
        "status": JobStates.pending.value,
        "model_id": 3,
        "priority": 1,
    }
    return job1, job2, job3, job4


@pytest.fixture
def populated_queue(queue, jobs, env_name):
    job1, job2, job3, job4 = jobs
    queue.enqueue(environment=env_name, item=job1)
    queue.enqueue(environment=env_name, item=job2)
    queue.enqueue(environment=None, item=job3)
    queue.enqueue(environment=f"not-{env_name}", item=job4)
    return queue


@pytest.fixture
def populated_full_queue(queue, full_jobs, env_name):
    job1, job2, job3, job4 = full_jobs
    queue.enqueue(environment=env_name, item=job1)
    queue.enqueue(environment=env_name, item=job2)
    queue.enqueue(environment=env_name, item=job3)
    queue.enqueue(environment=env_name, item=job4)
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
        return 'test-queue'

    def test_create_queue(self, queue, env_name):
        # test queue is empty
        assert queue.peek(hostname="i", environment=env_name)[0] is None

    def test_enqueue(self, populated_queue, env_name):
        assert populated_queue.peek(hostname="i", environment=env_name)[0] is not None

    def test_peek(self, populated_queue, env_name):
        assert populated_queue.peek(hostname="i", environment=env_name)[0]['model_id'] == 0
        # peeking does not remove elements
        assert populated_queue.peek(hostname="i", environment=env_name)[0]['model_id'] == 0
        
    def test_dequeue(self, populated_queue, env_name):
        assert populated_queue.dequeue(environment=env_name)['model_id'] == 0
        # dequeue removes elements
        assert populated_queue.dequeue(environment=env_name)['model_id'] == 1
        # None env jobs can be pulled by everyone
        assert populated_queue.dequeue(environment=None)['model_id'] == 2
        # environment is filtered correctly
        assert populated_queue.dequeue(environment=env_name) is None
    
    def test_dequeue_old(self, populated_queue, env_name):
        # old enough
        assert populated_queue.dequeue(environment=env_name, older_than=5*60) is not None
        # not old enough
        assert populated_queue.dequeue(environment=env_name, older_than=5*60) is None


#@pytest.mark.skip("too slow in development")
class TestsSQSQueue(TestLocalQueue):
    
    @pytest.fixture
    def queue(self, env_name):
        time.sleep(65)  # timeout to recreate queue
        # need new env name for each test (SQS queues can't be recreated after less than 60 seconds)
        job_queue = SQSJobQueue([env_name, f"not-{env_name}"])
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
    
    # need to override since RDS queue takes date of creation in DB
    def test_dequeue_old(self, populated_queue, env_name):
        # not old enough
        assert populated_queue.dequeue(environment=env_name, older_than=2) is None
        # old enough
        time.sleep(2)
        assert populated_queue.dequeue(environment=env_name, older_than=2) is not None
    
    # additional tests for additional functionality
    def test_filtering(self, populated_full_queue, env_name):
        assert populated_full_queue.peek(hostname="i", environment=env_name, cpu_cores=2)[0]["model_id"] == 1
        assert populated_full_queue.peek(hostname="i", environment=env_name, memory=1)[0]["model_id"] == 1
        assert populated_full_queue.peek(hostname="i", environment=env_name, gpu_model="gpu_model")[0]["model_id"] == 1
        assert populated_full_queue.peek(hostname="i", environment=env_name, gpu_archi="gpu_archi")[0]["model_id"] == 1
    
    def test_priorities(self, populated_full_queue, env_name):
        # group priority
        assert populated_full_queue.dequeue(environment=env_name, groups=["group", "another group"])["model_id"] == 2
        # job priority
        assert populated_full_queue.dequeue(environment=env_name)["model_id"] == 1
        assert populated_full_queue.dequeue(environment=env_name)["model_id"] == 3

