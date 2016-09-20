import rq

from rq_retry_scheduler import Queue, Worker


def noop_target_function(*args, **kwargs):
    pass


def fail_target_function(*args, **kwargs):
    raise Exception("I am a failure of a function")


def test_init(worker):
    assert worker.exc_handler in worker._exc_handlers
    assert issubclass(worker.queue_class, Queue)


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


def test_cli_arguments(connection):
    """
    The rq script populates the queue_class and instantiates the queues
    with the RQ queue class.

    Make sure that our worker changes the queue class and updates the
    queues to be of the new queue class
    """
    queue_names = ['unittest1', 'unittest2']
    queues = [
        rq.Queue(queue_name, connection=connection)
        for queue_name in queue_names
    ]

    w = Worker(queues, connection=connection, queue_class=rq.Queue)

    assert issubclass(w.queue_class, Queue)
    assert len(w.queues) == len(queues)

    for queue in w.queues:
        assert isinstance(queue, Queue), queue.name


def test_class_override_inherited(connection):
    """Test that passing a subclass of Queue isn't overwritten by the worker"""

    class UnittestQueue(Queue):
        pass

    w = Worker(['unittest'], queue_class=UnittestQueue, connection=connection)

    assert w.queue_class == UnittestQueue
    assert len(w.queues) == 1


def test_queue_strings(connection):
    """Ensure that the worker can take an iterable of strings"""

    queues = ['unittest']
    w = Worker(queues, queue_class=rq.Queue, connection=connection)

    assert issubclass(w.queue_class, Queue)
    assert len(w.queues) == len(queues)

    for queue in w.queues:
        assert isinstance(queue, Queue), queue.name


def test_queue_string(connection):
    """Ensure that the worker can take a string"""

    queues = 'unittest'
    w = Worker(queues, queue_class=rq.Queue, connection=connection)

    assert issubclass(w.queue_class, Queue)
    assert len(w.queues) == 1

    for queue in w.queues:
        assert isinstance(queue, Queue), queue.name
