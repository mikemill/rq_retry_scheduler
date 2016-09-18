import pytest
from redis import StrictRedis


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
        conn.flushall()
