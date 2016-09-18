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
    ret_value = "unittest"
    enqueue = mock.patch.object(q, 'enqueue', return_value=ret_value)

    dt = datetime(2016, 1, 1, 0, 0, 0)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}
    meta = {'enqueue_at': dt}

    j = q.enqueue_at(dt, target_function, *args, **kwargs)

    assert j == ret_value

    enqueue.assert_called_with(
        target_function, args=args, kwargs=kwargs, meta=meta)


def test_enqueue_in(mock, q):
    dt = datetime.utcnow().replace(microsecond=0)
    q.current_time = lambda: dt

    td = timedelta(minutes=5)

    ret_value = "unittest"
    enqueue = mock.patch.object(q, 'enqueue', return_value=ret_value)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}
    meta = {'enqueue_at': dt + td}

    j = q.enqueue_in(td, target_function, *args, **kwargs)

    assert j == ret_value

    enqueue.assert_called_with(
        target_function, args=args, kwargs=kwargs, meta=meta)


def test_enqueue_job_at(mock, q, connection):
    ret_value = "unittest"
    enqueue = mock.patch.object(q, 'enqueue_job', return_value=ret_value)

    dt = datetime(2016, 1, 1, 0, 0, 0)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}

    job = Job.create(
        target_function, args=args, kwargs=kwargs, connection=connection)

    assert 'enqueue_at' not in job.meta

    j = q.enqueue_job_at(dt, job)

    assert j == ret_value

    enqueue.assert_called_with(job)

    assert 'enqueue_at' in job.meta
    assert job.meta['enqueue_at'] == dt


def test_enqueue_job_in(mock, q, connection):
    dt = datetime.utcnow().replace(microsecond=0)
    q.current_time = lambda: dt

    td = timedelta(minutes=5)

    ret_value = 'unittest'
    enqueue = mock.patch.object(q, 'enqueue_job', return_value=ret_value)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}

    job = Job.create(
        target_function, args=args, kwargs=kwargs, connection=connection)

    assert 'enqueue_at' not in job.meta

    j = q.enqueue_job_in(td, job)

    assert j == ret_value

    enqueue.assert_called_with(job)

    assert 'enqueue_at' in job.meta
    assert job.meta['enqueue_at'] == dt + td


def test_schedule_job(mock, q, connection):
    zadd = mock.patch.object(connection, '_zadd')

    job = Job.create(target_function, connection=connection)

    save = mock.patch.object(job, 'save')
    dt = datetime.utcnow()

    q.schedule_job(job, dt)

    zadd.assert_called_with(q.scheduler_jobs_key, util.to_unix(dt), job.id)
    save.assert_called()


def test_enqueue_job(mock, q, connection):
    dt = datetime.utcnow()

    job = Job.create(target_function, connection=connection)
    job.meta['enqueue_at'] = dt

    schedule = mock.patch.object(q, 'schedule_job')

    j = q.enqueue_job(job)

    assert j == job
    schedule.assert_called_with(job, dt)


def test_enqueue_job_no_time(mock, q, connection):
    job = Job.create(target_function, connection=connection)

    enqueue = mock.patch.object(queue.rq.Queue, 'enqueue_job')
    schedule = mock.patch.object(q, 'schedule_job')

    q.enqueue_job(job)

    enqueue.assert_called_with(job, None, False)
    assert not schedule.called
