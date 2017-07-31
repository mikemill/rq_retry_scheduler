from datetime import datetime
import pytest
from redis import StrictRedis

from rq_retry_scheduler import Queue, Worker, Scheduler


@pytest.fixture(scope='session')
def redis_db_num():
    """Find an empty Redis database to use"""
    for dbnum in range(4, 17):
        conn = StrictRedis(db=dbnum)
        if len(conn.keys('*')) == 0:
            return dbnum
    assert False, "Couldn't find an empty Redis DB"


@pytest.yield_fixture
def connection(redis_db_num):
    conn = StrictRedis(db=redis_db_num)
    try:
        yield conn
    finally:
        conn.flushdb()


@pytest.yield_fixture
def queue(connection):
    q = Queue('unittest', connection=connection)
    try:
        yield q
    finally:
        q.current_time = datetime.utcnow


@pytest.yield_fixture
def queue2(connection):
    q = Queue('unittest2', connection=connection)
    try:
        yield q
    finally:
        q.current_time = datetime.utcnow


@pytest.yield_fixture
def scheduler(connection):
    s = Scheduler(connection=connection)
    try:
        yield s
    finally:
        s.current_time = datetime.utcnow


@pytest.yield_fixture
def worker(connection):
    def blackhole(job, *args, **kwargs):
        """For unit tests we don't want it going to default behavior"""
        return False

    w = Worker(
        'unittest', connection=connection, queue_class=Queue,
        exception_handlers=blackhole)

    try:
        yield w
    finally:
        pass
