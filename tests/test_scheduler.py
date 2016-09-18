from datetime import datetime
import pytest
from rq.exceptions import NoSuchJobError
from rq.job import Job
import time

from rq_retry_scheduler.scheduler import Scheduler, Queue
from rq_retry_scheduler.util import to_unix


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

    def test(self):
        pass


def target_function(*args, **kwargs):
    pass


@pytest.yield_fixture
def scheduler(connection):
    s = Scheduler('unittest', connection=connection)
    try:
        yield s
    finally:
        s.current_time = datetime.utcnow


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


def test_get_jobs_to_queue(scheduler, mock):
    job_ids = ['1', '2', '3', '4']

    zrange = mock.patch.object(scheduler.connection, 'zrangebyscore',
                               return_value=job_ids)

    get_job = mock.patch.object(scheduler, 'get_job', return_value=MockJob())

    jobs = list(scheduler.get_jobs_to_queue(99))

    assert zrange.called
    assert get_job.call_count == len(job_ids)
    assert len(jobs) == len(job_ids)


def test_get_jobs_to_queue_no_such_job(scheduler, mock):
    job_ids = ['1']

    mock.patch.object(scheduler.connection, 'zrangebyscore',
                      return_value=job_ids)

    mock.patch.object(scheduler, 'get_job', side_effect=NoSuchJobError)

    remove_job = mock.patch.object(scheduler, 'remove_job')

    jobs = list(scheduler.get_jobs_to_queue(99))

    assert remove_job.called_with(job_ids[0])
    assert len(jobs) == 0


def test_enqueue_jobs(scheduler, mock):
    job_origins = ['unittest'] * 3 + ['unittest2'] * 2

    jobs = [
        Job.create(target_function, connection=scheduler.connection,
                   id=str(job_id + 1), origin=origin)
        for job_id, origin in enumerate(job_origins)
    ]

    get_jobs = mock.patch.object(scheduler, 'get_jobs_to_queue',
                                 return_value=jobs)

    enqueue = mock.patch.object(Queue, 'enqueue_job')

    dt = datetime.utcnow().replace(microsecond=0)
    scheduler.current_time = lambda: dt

    scheduler.enqueue_jobs()

    assert get_jobs.called_with(to_unix(dt))
    assert enqueue.call_count == len(jobs)


def test_run(scheduler, mock):
    side_effects = [None, Exception('End')]

    enqueue_jobs = mock.patch.object(
        scheduler, 'enqueue_jobs', side_effect=side_effects)

    mock.patch.object(time, 'sleep')  # Don't actually sleep in unittests

    with pytest.raises(Exception):
        scheduler.run()

    assert enqueue_jobs.call_count == len(side_effects)
