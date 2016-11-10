from datetime import datetime, timedelta
import re

class DateUtil:

    '''
    Convert a python date object to a unix timestamp in microseconds
    input: Python date object
    output: unix timestamp: ex: 1475409600000
    '''
    @staticmethod
    def date_to_timestamp(dt, epoch=datetime(1970,1,1)):
        td = dt - epoch
        # return td.total_seconds()
        return int((td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6) * 1000

    '''
    Parse a date string according to the date format and then return the
    unix timestamp in microseconds dimension
    input: date string, format used for parsing date string. example: "2016-10-02T12:00:00+00:00", "%Y-%m-%dT%H:%M:%S%Z"
    output: unix timestamp: ex: 1475409600000
    '''
    @staticmethod
    def unix_timestamp(date_str, date_format):

        date_str = date_str.strip()

        if date_format.find('%Z') != -1:
            date_format = date_format.replace('%Z', '')
            match_object = re.search('(([-+])(\d{2})(\d{2}))', date_str)
            if match_object is None:
                match_object = re.search('(([-+])(\d{2}):(\d{2}))', date_str)
            tz = match_object.groups()

            dt = datetime.strptime(date_str.replace(tz[0], ''), date_format)
            delta = timedelta(hours=int(tz[2]), minutes=int(tz[3]))
            if tz[1] == '-': delta = delta * -1
            dt = dt + delta

            return DateUtil.date_to_timestamp(dt)

        date_obj = datetime.strptime(date_str, date_format)
        return DateUtil.date_to_timestamp(date_obj)
