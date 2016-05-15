# coding=utf8
from datetime import datetime
import time
import xlrd


def datetime2utc(dt):
    return time.mktime(dt.timetuple())


def utc2datetime(utc):
    return datetime.fromtimestamp(utc)


def serialize_to(obj, tobj):
    tobj = tobj()
    for k, v in tobj.__dict__.iteritems():
        value = getattr(obj, k)
        print value
        if isinstance(value, datetime):
            value = datetime2utc(value)
        if isinstance(value, unicode):
            value = value.encode('utf8')
        setattr(tobj, k, value)
    return tobj


def get_key_mood_map(path, name):
    data = xlrd.open_workbook(path + name)
    key_mood_map = {}
    for sheet in [0]:
        table = data.sheets()[sheet]
        for nrow in range(0, table.nrows):
            key_mood_map[table.row_values(nrow)[0]] = \
                {
                    "_class": table.row_values(nrow)[1],
                    "strength": table.row_values(nrow)[2]
            }
    return key_mood_map
