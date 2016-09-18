from datetime import datetime, timedelta

from rq_retry_scheduler import queue


def target_function(*args, **kwargs):
    pass


def test_logger_name():
    assert queue.logger.name == 'rq:retryscheduler:queue'


def test_job_key():
    assert queue.Queue.scheduler_jobs_key == 'rq:retryscheduler:scheduled_jobs'


def test_enqueue_at(mock, connection):
    q = queue.Queue('unittest', connection=connection)

    enqueue = mock.patch.object(q, 'enqueue')

    dt = datetime(2016, 1, 1, 0, 0, 0)

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}
    meta = {'enqueue_at': dt}

    q.enqueue_at(dt, target_function, *args, **kwargs)

    enqueue.assert_called_with(
        target_function, args=args, kwargs=kwargs, meta=meta)


def test_enqueue_in(mock, connection):
    q = queue.Queue('unittest', connection=connection)

    dt = datetime.utcnow().replace(microsecond=0)
    td = timedelta(minutes=5)

    # Mock the entire datetime import
    dt_mock = mock.patch.object(queue, 'datetime', autospec=True)

    # And now mock the mock'd class
    mock.patch.object(dt_mock, 'utcnow', return_value=dt)

    enqueue = mock.patch.object(q, 'enqueue')

    args = 'unit', 'tests'
    kwargs = {'are': 'cool'}
    meta = {'enqueue_at': dt + td}

    q.enqueue_in(td, target_function, *args, **kwargs)

    enqueue.assert_called_with(
        target_function, args=args, kwargs=kwargs, meta=meta)
