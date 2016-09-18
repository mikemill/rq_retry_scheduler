from rq_retry_scheduler import Queue


def noop_target_function(*args, **kwargs):
    pass


def fail_target_function(*args, **kwargs):
    raise Exception("I am a failure of a function")


def test_init(worker):
    assert worker.exc_handler in worker._exc_handlers


def test_exc_handler(mock, worker, queue):
    job = queue.enqueue(fail_target_function)

    enqueue = mock.patch.object(Queue, 'enqueue_job_in')

    for count, delay in worker.retry_delays.items():
        ret = worker.exc_handler(job, None, None, None)
        assert ret is False
        enqueue.assert_called_with(delay, job)
        enqueue.reset_mock()

    # Now test that it didn't retry
    ret = worker.exc_handler(job, None, None, None)
    assert ret is True
    assert not enqueue.called
