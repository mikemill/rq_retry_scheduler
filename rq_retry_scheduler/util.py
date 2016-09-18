import calendar


def to_unix(dt):
    return calendar.timegm(dt.utctimetuple())
