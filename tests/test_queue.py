from datetime import datetime, timedelta
import rq
from rq.job import Job

from rq_retry_scheduler import queue, util


def target_function(*args, **kwargs):
    pass


def test_logger_name():
    assert queue.logger.name == 'rq:retryscheduler:queue'


def test_job_key():
    assert queue.Queue.scheduler_jobs_key == 'rq:retryscheduler:scheduled_jobs'


def test_enqueue_at(mock, queue):
    ret_value = "unittest"
    enqueue = mock.patch.object(queue, 'enqueue', return_value=ret_value)

    dt = datetime(2016, 1, 1, 0, 0, 0)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}
    meta = {'enqueue_at': dt}

    j = queue.enqueue_at(dt, target_function, *args, **kwargs)

    assert j == ret_value

    enqueue.assert_called_with(
        target_function, args=args, kwargs=kwargs, meta=meta)


def test_enqueue_in(mock, queue):
    dt = datetime.utcnow().replace(microsecond=0)
    queue.current_time = lambda: dt

    td = timedelta(minutes=5)

    ret_value = "unittest"
    enqueue = mock.patch.object(queue, 'enqueue', return_value=ret_value)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}
    meta = {'enqueue_at': dt + td}

    j = queue.enqueue_in(td, target_function, *args, **kwargs)

    assert j == ret_value

    enqueue.assert_called_with(
        target_function, args=args, kwargs=kwargs, meta=meta)


def test_enqueue_job_at(mock, queue, connection):
    ret_value = "unittest"
    enqueue = mock.patch.object(queue, 'enqueue_job', return_value=ret_value)

    dt = datetime(2016, 1, 1, 0, 0, 0)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}

    job = Job.create(
        target_function, args=args, kwargs=kwargs, connection=connection)

    assert 'enqueue_at' not in job.meta

    j = queue.enqueue_job_at(dt, job)

    assert j == ret_value

    enqueue.assert_called_with(job)

    assert 'enqueue_at' in job.meta
    assert job.meta['enqueue_at'] == dt


def test_enqueue_job_in(mock, queue, connection):
    dt = datetime.utcnow().replace(microsecond=0)
    queue.current_time = lambda: dt

    td = timedelta(minutes=5)

    ret_value = 'unittest'
    enqueue = mock.patch.object(queue, 'enqueue_job', return_value=ret_value)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}

    job = Job.create(
        target_function, args=args, kwargs=kwargs, connection=connection)

    assert 'enqueue_at' not in job.meta

    j = queue.enqueue_job_in(td, job)

    assert j == ret_value

    enqueue.assert_called_with(job)

    assert 'enqueue_at' in job.meta
    assert job.meta['enqueue_at'] == dt + td


def test_schedule_job(mock, queue, connection):
    zadd = mock.patch.object(connection, '_zadd')

    job = Job.create(target_function, connection=connection)

    save = mock.patch.object(job, 'save')
    dt = datetime.utcnow()

    queue.schedule_job(job, dt)

    zadd.assert_called_with(queue.scheduler_jobs_key, util.to_unix(dt), job.id)
    assert save.called
    assert job.meta.get('scheduled_for') == dt


def test_enqueue_job(mock, queue, connection):
    dt = datetime.utcnow()

    job = Job.create(target_function, connection=connection)
    job.meta['enqueue_at'] = dt

    schedule = mock.patch.object(queue, 'schedule_job')

    j = queue.enqueue_job(job)

    assert j == job
    schedule.assert_called_with(job, dt)


def test_enqueue_job_no_time(mock, queue, connection):
    job = Job.create(target_function, connection=connection)

    enqueue = mock.patch.object(rq.Queue, 'enqueue_job')
    schedule = mock.patch.object(queue, 'schedule_job')

    queue.enqueue_job(job)

    enqueue.assert_called_with(job, None, False)
    assert not schedule.called


def test_contains(queue, connection):
    job = Job.create(target_function, connection=connection)
    assert job not in queue
    assert job.id not in queue
    assert 'foobar' not in queue

    queue.enqueue_job_in(timedelta(seconds=1), job)
    assert job in queue
    assert job.id in queue
    assert 'foobar' not in queue


def test_scheduled_jobs(queue, queue2):
    args = list(range(0, 10))

    queued_jobs = [
        queue.enqueue_in(timedelta(seconds=arg), target_function, arg)
        for arg in args]

    [queue2.enqueue_in(timedelta(seconds=arg), target_function, arg)
     for arg in args]

    jobs = list(queue.scheduled_jobs())

    assert len(jobs) == len(args)

    job_args = zip(queued_jobs, jobs)

    for qjob, job in job_args:
        assert qjob.id == job.id


def test_unschedule_job(queue):
    td = timedelta(seconds=1)
    job1 = queue.enqueue_in(td, target_function)
    job2 = queue.enqueue_in(td, target_function)

    assert job1 in queue
    assert job2 in queue

    queue.unschedule_job(job1)

    assert job1 not in queue
    assert job2 in queue

    queue.unschedule_job(job2.id)

    assert job2 not in queue


def test_repeat_job(queue):
    td = timedelta(seconds=1)
    job = queue.enqueue_in(td, target_function)
    assert queue.repeat_job(job, td) is job

    j2 = Job.fetch(job.id, connection=job.connection)
    assert job == j2
    assert j2.meta['interval'] == td
    assert j2.meta['max_runs'] is None
    assert j2.meta['run_count'] == 0


def test_repeat_job_not_in_queue(queue, mock):
    td = timedelta(seconds=1)

    job = Job.create(target_function, connection=queue.connection)
    s = mock.spy(queue, 'enqueue_job_in')

    queue.repeat_job(job, td)
    assert job in queue
    s.assert_called_with(td, job)
