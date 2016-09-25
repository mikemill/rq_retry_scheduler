from datetime import datetime

from rq_retry_scheduler import util

try:
    from datetime import timezone, timedelta
    utc = timezone.utc
    pdt = timezone(timedelta(hours=-7))
except ImportError:
    import pytz
    utc = pytz.utc
    pdt = pytz.timezone('Etc/GMT+7')  # POSIX flips the sign


def test_to_unix():
    tests = {
        datetime(1970, 1, 1, 0, 0, 0, tzinfo=utc): 0,
        datetime(2038, 1, 19, 3, 14, 7, tzinfo=utc): 2147483647,
        datetime(2016, 9, 17, 11, 26, 48, tzinfo=pdt): 1474136808,
    }

    for dt, ts in tests.items():
        assert util.to_unix(dt) == ts, dt
