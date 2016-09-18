from datetime import datetime, timezone, timedelta

from rq_retry_scheduler import util


def test_to_unix():
    pdt = timezone(timedelta(hours=-7))

    tests = {
        datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc): 0,
        datetime(2038, 1, 19, 3, 14, 7, tzinfo=timezone.utc): 2147483647,
        datetime(2016, 9, 17, 11, 26, 48, tzinfo=pdt): 1474136808,
    }

    for dt, ts in tests.items():
        assert util.to_unix(dt) == ts, dt
