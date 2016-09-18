from datetime import datetime, timedelta
from rq.job import Job
import pytest

from rq_retry_scheduler import queue, util


@pytest.fixture
def q(connection):
    return queue.Queue('unittest', connection=connection)


def target_function(*args, **kwargs):
    pass


def test_logger_name():
    assert queue.logger.name == 'rq:retryscheduler:queue'


def test_job_key():
    assert queue.Queue.scheduler_jobs_key == 'rq:retryscheduler:scheduled_jobs'


def test_enqueue_at(mock, q):
    enqueue = mock.patch.object(q, 'enqueue')

    dt = datetime(2016, 1, 1, 0, 0, 0)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}
    meta = {'enqueue_at': dt}

    q.enqueue_at(dt, target_function, *args, **kwargs)

    enqueue.assert_called_with(
        target_function, args=args, kwargs=kwargs, meta=meta)


def test_enqueue_in(mock, q):
    dt = datetime.utcnow().replace(microsecond=0)
    q.current_time = lambda: dt

    td = timedelta(minutes=5)

    enqueue = mock.patch.object(q, 'enqueue')

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}
    meta = {'enqueue_at': dt + td}

    q.enqueue_in(td, target_function, *args, **kwargs)

    enqueue.assert_called_with(
        target_function, args=args, kwargs=kwargs, meta=meta)


def test_enqueue_job_at(mock, q, connection):
    enqueue = mock.patch.object(q, 'enqueue_job')

    dt = datetime(2016, 1, 1, 0, 0, 0)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}

    job = Job.create(
        target_function, args=args, kwargs=kwargs, connection=connection)

    assert 'enqueue_at' not in job.meta

    q.enqueue_job_at(dt, job)

    enqueue.assert_called_with(job)

    assert 'enqueue_at' in job.meta
    assert job.meta['enqueue_at'] == dt


def test_enqueue_job_in(mock, q, connection):
    dt = datetime.utcnow().replace(microsecond=0)
    q.current_time = lambda: dt

    td = timedelta(minutes=5)

    enqueue = mock.patch.object(q, 'enqueue_job')

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}

    job = Job.create(
        target_function, args=args, kwargs=kwargs, connection=connection)

    assert 'enqueue_at' not in job.meta

    q.enqueue_job_in(td, job)

    enqueue.assert_called_with(job)

    assert 'enqueue_at' in job.meta
    assert job.meta['enqueue_at'] == dt + td
