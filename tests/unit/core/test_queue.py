import datetime
import pytest
import random
import threading
import time
from workerfacing_api.core.queue import LocalJobQueue, SQSJobQueue, RDSJobQueue


@pytest.fixture
def skip_aws_mock(env):
    if env == "aws_mock":
        pytest.skip("Missing attribute 'some_attr'")
    yield env


@pytest.fixture(scope="function")
def jobs():
    time_now = datetime.datetime.utcnow().isoformat()
    common_base = {
        "app": {"application": "app", "version": "v", "entrypoint": "e"},
        "handler": {"image_url": "u"},
    }
    job0 = {"job": {**common_base, "meta": {"job_id": 0, "date_created": time_now}}, "paths_upload": {}}
    job1 = {"job": {**common_base, "meta": {"job_id": 1, "date_created": time_now}}, "paths_upload": {}}
    job2 = {"job": {**common_base, "meta": {"job_id": 2, "date_created": time_now}}, "paths_upload": {}}
    job3 = {"job": {**common_base, "meta": {"job_id": 3, "date_created": time_now}}, "paths_upload": {}}
    return job0, job1, job2, job3


@pytest.fixture(scope="function")
def full_jobs(jobs):
    job0 = {
        **jobs[0],
        "hardware": {
            "cpu_cores": 3,
            "memory": 2,
            "gpu_model": "gpu_model",
            "gpu_archi": "gpu_archi",
            "gpu_mem": 0,
        },
        "group": None,
        "priority": 5,
    }
    job1 = {
        **jobs[1],
        "hardware": {
            "cpu_cores": 1,
            "memory": 0,
            "gpu_model": None,
            "gpu_archi": None,
            "gpu_mem": None,
        },
        "group": None,
        "priority": 10,
    }
    job2 = {
        **jobs[2],
        "hardware": {},
        "group": "group",
        "priority": 1,
    }
    job3 = {
        **jobs[3],
        "hardware": {},
        "priority": 1,
    }
    return job0, job1, job2, job3


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

    @pytest.fixture(scope="function")
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
        assert populated_queue.peek(hostname="i", environment=env_name)[0]['meta']['job_id'] == 0
        # peeking does not remove elements
        assert populated_queue.peek(hostname="i", environment=env_name)[0]['meta']['job_id'] == 0
        
    def test_dequeue(self, populated_queue, env_name):
        assert populated_queue.dequeue(hostname="i", environment=env_name)['meta']['job_id'] == 0
        # dequeue removes elements
        assert populated_queue.dequeue(hostname="i", environment=env_name)['meta']['job_id'] == 1
        # None env jobs can be pulled by everyone
        assert populated_queue.dequeue(hostname="i", environment=None)['meta']['job_id'] == 2
        # environment is filtered correctly
        assert populated_queue.dequeue(hostname="i", environment=env_name) is None
    
    def test_dequeue_old(self, populated_queue, env_name):
        # not old enough
        assert populated_queue.dequeue(hostname="i", environment=env_name, older_than=2) is None
        # old enough
        time.sleep(2)
        assert populated_queue.dequeue(hostname="i", environment=env_name, older_than=2) is not None
    
    def test_concurrent_pulls(self, populated_full_queue, env_name):
        found_jobs = [None] * 100

        def _pull(i):
            time.sleep(random.random() * 5)  # ensure enough time to pull all jobs
            found_jobs[i] = populated_full_queue.dequeue(hostname="i", environment=env_name)

        threads = []
        for i in range(100):
            thread = threading.Thread(target=_pull, args=(i,))
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        found_jobs = [job for job in found_jobs if job is not None]
        # no duplicates
        assert len(found_jobs) == len(set([job["meta"]["job_id"] for job in found_jobs]))


#@pytest.mark.skip("too slow in development")
class TestsSQSQueue(TestLocalQueue):
    
    @pytest.fixture
    def queue(self, env_name, skip_aws_mock):
        # need new env name for each test (SQS queues can't be recreated after less than 60 seconds)
        job_queue = SQSJobQueue([env_name, f"not-{env_name}"])
        # wait for queue to be deleted before recreating
        while True:
            try:
                job_queue.create(err_on_exists=True)
                break
            except:
                time.sleep(1)
        yield job_queue
        job_queue.delete()


def _patched_func(queue, func_name):
    to_patch = getattr(queue, func_name)
    def patch(*args, **kwargs):
        for required_arg in ("cpu_cores", "memory", "gpu_mem"):
            if required_arg not in kwargs:
                kwargs[required_arg] = 9999
        return to_patch(*args, **kwargs)
    return patch


@pytest.fixture(autouse=True, scope="module")
def patch_queue_funcs(monkeypatch_module):
    monkeypatch_module.setattr(RDSJobQueue, "peek", _patched_func(RDSJobQueue, "peek"))
    monkeypatch_module.setattr(RDSJobQueue, "dequeue", _patched_func(RDSJobQueue, "dequeue"))


class TestRDSLocalQueue(TestLocalQueue):
    
    @pytest.fixture(scope="function")
    def queue(self, tmpdir, env_name):
        job_queue = RDSJobQueue(f"sqlite:///{tmpdir}/{env_name}.db")
        job_queue.create()
        yield job_queue
        job_queue.delete()
    
    # additional tests for additional functionality
    def test_filtering(self, populated_full_queue, env_name):
        assert populated_full_queue.peek(hostname="i", environment=env_name, cpu_cores=2, memory=1)[0]["meta"]["job_id"] == 1
        assert populated_full_queue.peek(hostname="i", environment=env_name, memory=1)[0]["meta"]["job_id"] == 1
        assert populated_full_queue.peek(hostname="i", environment=env_name, gpu_model="gpu_model")[0]["meta"]["job_id"] == 1
        assert populated_full_queue.peek(hostname="i", environment=env_name, gpu_archi="gpu_archi")[0]["meta"]["job_id"] == 1
    
    def test_priorities(self, populated_full_queue, env_name):
        # group priority
        assert populated_full_queue.dequeue(hostname="i", environment=env_name, groups=["group", "another group"])["meta"]["job_id"] == 2
        # job priority
        assert populated_full_queue.dequeue(hostname="i", environment=env_name)["meta"]["job_id"] == 1
        assert populated_full_queue.dequeue(hostname="i", environment=env_name)["meta"]["job_id"] == 3

    def test_dequeue_old(self, populated_queue, env_name):
        # older_than does not apply when the right environment is selected
        # not old enough
        assert populated_queue.dequeue(hostname="i", environment=env_name, older_than=5) is not None
        assert populated_queue.dequeue(hostname="i", environment=env_name, older_than=5) is not None
        assert populated_queue.dequeue(hostname="i", environment=env_name, older_than=5) is None
        # old enough
        time.sleep(5)
        assert populated_queue.dequeue(hostname="i", environment=env_name, older_than=5) is not None

    def test_failures(self, populated_full_queue, env_name):
        job_id = populated_full_queue.dequeue(hostname="first", environment=env_name)["job_id"]
        res = populated_full_queue.dequeue(hostname="second", environment=env_name)
        assert not res or res["job_id"] != job_id
        # fail -> requeue
        time.sleep(6)
        populated_full_queue.handle_timeouts(max_retries=1, timeout_failure=5)
        assert populated_full_queue.get_job(job_id).status == "queued"
        # same worker can not repull
        res = populated_full_queue.dequeue(hostname="first", environment=env_name)
        assert not res or res["job_id"] != job_id
        # different worker can repull
        res = populated_full_queue.dequeue(hostname="second", environment=env_name)
        assert res["job_id"] == job_id
        # fail
        time.sleep(6)
        populated_full_queue.handle_timeouts(max_retries=1, timeout_failure=5)
        assert populated_full_queue.get_job(job_id).status == "error"
