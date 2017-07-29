from argparse import Namespace
import logging
from redis import StrictRedis

from rq_retry_scheduler import Scheduler
from rq_retry_scheduler.cli import scheduler


def test_get_redis():
    args = Namespace(
        host='localhost', port=6379, db=15, password=None, url=None)
    redis = scheduler.get_redis(args)
    assert isinstance(redis, StrictRedis)

    args = Namespace(
        url='redis://localhost/15')

    redis = scheduler.get_redis(args)
    assert isinstance(redis, StrictRedis)


def test_setup_logging():
    args = Namespace()

    logger = scheduler.setup_logging(args)

    assert logger.getEffectiveLevel() == logging.INFO
    assert len(logger.handlers) > 0

    args = Namespace(loglevel='ERROR')
    logger = scheduler.setup_logging(args)

    assert logger.getEffectiveLevel() == logging.ERROR


def test_main(mock):
    args = Namespace(url='redis://localhost/15', interval=5, burst=False)

    mock.patch.object(scheduler, 'get_arguments', return_value=args)
    init = mock.spy(Scheduler, '__init__')
    run = mock.patch.object(Scheduler, 'run')

    scheduler.main()

    run.assert_called_with(False)

    assert init.call_args[1]['interval'] == args.interval


def test_get_arguments():
    fake_arguments = []
    args = scheduler.get_arguments(fake_arguments)
    assert args.host == 'localhost'
    assert args.port == 6379
    assert args.url is None
    assert args.interval == 10.0
    assert args.burst is False


def test_burst_flag():
    fake_arguments = ['-b']
    args = scheduler.get_arguments(fake_arguments)
    assert args.burst is True
