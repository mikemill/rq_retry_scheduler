import pytest

from rq_retry_scheduler.scheduler import Scheduler


class MockJob(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def fetch(self, *args, **kwargs):
        return args, kwargs


class MockQueue(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


@pytest.fixture
def scheduler(connection):
    return Scheduler('unittest', connection=connection)


def test_get_queue(connection):
    s = Scheduler('unitest', queue_class=MockQueue, connection=connection)

    q = s.get_queue('unittest')

    assert isinstance(q, MockQueue)

    q2 = s.get_queue('unittest')

    assert q2 is q


def test_remove_job(mock, scheduler):
    zrem = mock.patch.object(scheduler.connection, 'zrem')

    job_id = 'unittest'

    scheduler.remove_job(job_id)

    zrem.assert_called_with(scheduler.scheduler_jobs_key, job_id)


def test_get_job(scheduler):
    scheduler.job_class = MockJob

    job_id = 'unittest'
    args, kwargs = scheduler.get_job(job_id)

    assert args == (job_id,)
    assert kwargs == {'connection': scheduler.connection}


def test_get_job_bytes(scheduler):
    scheduler.job_class = MockJob

    job_id = 'unittest'
    args, kwargs = scheduler.get_job(job_id.encode('utf-8'))

    assert args == (job_id,)
    assert kwargs == {'connection': scheduler.connection}
