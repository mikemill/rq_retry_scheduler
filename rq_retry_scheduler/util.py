import calendar
from datetime import datetime


def to_unix(dt):
    return calendar.timegm(dt.utctimetuple())
