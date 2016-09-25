from datetime import datetime, timedelta
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


def test_enqueue_jobs_with_repeat(scheduler, mock):
    job = Job.create(target_function, connection=scheduler.connection)

    m = mock.patch.object(scheduler, 'handle_job_repeat')
    mock.patch.object(scheduler, 'is_repeat', return_value=True)
    mock.patch.object(scheduler, 'get_jobs_to_queue',
                      return_value=[job])

    scheduler.enqueue_jobs()
    assert m.called


def test_run(scheduler, mock):
    side_effects = [None, Exception('End')]

    enqueue_jobs = mock.patch.object(
        scheduler, 'enqueue_jobs', side_effect=side_effects)

    mock.patch.object(time, 'sleep')  # Don't actually sleep in unittests

    with pytest.raises(Exception):
        scheduler.run()

    assert enqueue_jobs.call_count == len(side_effects)


def test_run_burst(scheduler, mock):
    sleep = mock.patch.object(time, 'sleep')

    scheduler.run(True)

    assert not sleep.called


def test_is_repeat(scheduler):
    job = Job.create(target_function, connection=scheduler.connection,
                     origin='unittest')

    assert scheduler.is_repeat(job) is False

    job.meta['interval'] = 10

    assert scheduler.is_repeat(job) is True


def test_delay_job(scheduler, queue):
    td = timedelta(seconds=3)
    dt = datetime.utcnow()

    queue.current_time = lambda: dt

    job = queue.enqueue_in(td, target_function)

    conn = queue.connection
    jobs = conn.zrange(queue.scheduler_jobs_key, 0, -1, withscores=True,
                       score_cast_func=int)
    _, ts = jobs[0]

    assert to_unix(dt + td) == ts

    scheduler.delay_job(job, td)

    jobs = conn.zrange(queue.scheduler_jobs_key, 0, -1, withscores=True,
                       score_cast_func=int)
    _, ts = jobs[0]

    assert to_unix(dt + td + td) == ts


def test_should_repeat_job(scheduler):
    job = Job.create(target_function, connection=scheduler.connection)
    job.meta.update({
        'interval': 1,
        'max_runs': None,
        'run_count': 99999,
    })

    # (max_runs, run_count, result)
    tests = [
        (None, 0, True),
        (None, 1, True),
        (None, 999999999, True),
        (1, 0, True),
        (1, 1, False),
        (1, 2, False),
    ]

    for max_runs, run_count, result in tests:
        job.meta['max_runs'] = max_runs
        job.meta['run_count'] = run_count
        assert scheduler.should_repeat_job(job) is result


def test_handle_job_repeat(scheduler, queue):
    td = timedelta(seconds=3)
    dt = datetime.utcnow()

    scheduler.current_time = lambda: dt
    queue.current_time = lambda: dt

    max_runs = 3

    job = queue.enqueue_in(td, target_function)
    queue.repeat_job(job, td, max_runs=max_runs)

    work_queue = Queue(job.origin, connection=queue.connection)

    assert job in queue
    assert job.id not in work_queue.get_job_ids()

    for r in range(1, max_runs + 1):
        repeat_job = scheduler.handle_job_repeat(job, work_queue)
        assert repeat_job not in queue
        assert repeat_job.id in work_queue.get_job_ids()
        assert job.meta['run_count'] == r

    assert job not in queue


def test_make_repat_job(scheduler):
    job = Job.create(target_function, args=(1, 2, 3), kwargs={'unit': 'test'},
                     connection=scheduler.connection)

    repeat_job = scheduler.make_repeat_job(job)

    assert repeat_job is not job
    assert repeat_job.id != job.id
    assert repeat_job.id == scheduler.repeat_job_id(job)
    assert repeat_job.parent is job
    assert repeat_job.func == job.func
    assert repeat_job.args == job.args
    assert repeat_job.kwargs == job.kwargs
