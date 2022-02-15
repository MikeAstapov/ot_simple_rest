import unittest
from datetime import datetime, timezone, timedelta

from parsers.otl_to_sparksql.internal.timerange import OTLTimeRangeExtractor


class TestOTLTimeRangeExtractor(unittest.TestCase):

    tz = timezone(timedelta(hours=+3))
    current_time = datetime(2010, 10, 10, 10, 10, 10, tzinfo=tz)
    OTLTR = OTLTimeRangeExtractor(current_datetime=current_time, tz=tz)

    # correct otl lines
    otl_request_correct = {
        'src': [
            '| otstats abrakadabra latest=now()',  # check now()
            '| otstats ... latest=now',  # check now
            '| otstats ... earliest=1111111100 latest=1111111111',  # check earliest < latest and timestamp
            '| otstats ... earliest=10/27/2015:00:00:00 latest="10/27/2018:00:00:00"',
            # check earliest < latest and
            # datetime format
            '| otstats ... earliest=10/27/2015:00:00:00 latest=10/27/2013:00:00:00',  # check earliest > latest
            '| otstats ... latest=-s',  # check seconds
            '| otstats ... latest=-m',  # check minutes
            '| otstats ... latest=-d',  # check days
            '| otstats ... latest=-w',  # check weeks
            '| otstats ... latest=-mon',  # check months
            '| otstats ... latest=-q',  # check quarter
            '| otstats ... latest=-y',  # check years
            '| otstats ... latest=+mon',  # check +, -
            '| otstats ... latest=@d',  # check @
            '| otstats ... latest=@y',  # check @
            '| otstats ... latest=@w',  # check @ nonstandard
            '| otstats ... latest=@w5',  # check @ nonstandard
            '| otstats ... latest=@w0',  # check @ nonstandard
            '| otstats ... latest=@q',  # check @ nonstandard
            '| otstats ... latest=@q2',  # check @ nonstandard
            '| otstats ... latest=@w9',  # check @ nonstandard + bad value
            '| otstats ... latest=@q5',  # check @ nonstandard + bad value
            '| otstats ... latest=@y5',  # check @ + unexpected value
            '| otstats ... latest=-1w2d',  # check hard expression without @
            '| otstats ... latest=-1w2d+1y',  # check hard expression without @
            '| otstats ... latest=-1w2d+1y-3mon',  # check hard expression without @
            '| otstats ... latest=-1w2d+1y-3mon+10m5s',  # check hard expression without @
            '| otstats ... latest=1w3d+1y-3mon+10m5s-1h',  # check hard expression without @
            '| otstats ... latest=-mon@d',  # check hard expression with @
            '| otstats ... latest=-12mon@d',  # check hard expression with @
            '| otstats ... latest=-12mon1y@d',  # check hard expression with @
            '| otstats ... latest=-12mon1y+10m@d',  # check hard expression with @
            '| otstats ... latest=-12mon1y@m',  # check hard expression with @
            '| otstats ... latest=-12mon1y@y',  # check hard expression with @
            '| otstats ... latest=-12mon1y@y+10d',  # check hard expression with @
            '| otstats ... latest=-12mon1y@y+10d-w',  # check hard expression with @
            '| otstats ... latest=-12mon1y@y+10d-w@w',  # check hard expression with @
            '| otstats ... latest=-12mon1y@y+10d-w@w2',  # check hard expression with @
            '| otstats ... latest=-12mon1y@y+10d-w@w2@w',  # check hard expression with @
            '| otstats ... latest=-12mon1y@y+10d-w@w2@w@q',  # check hard expression with @
            '| otstats ... latest=-12mon1y@y+10d-w@w2@w@q3',  # check hard expression with @
            '| otstats ... latest=-12mon1y@y+10d-w@w2@w@q3@q-1d@q3232-1s@q+1m-1h@q',  # check hard expression with @
        ],
        'dst': [
            '1970-01-01 03:00:00+03:00, 2010-10-10 10:10:10+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-10 10:10:10+03:00',
            '2005-03-18 04:58:20+03:00, 2005-03-18 04:58:31+03:00',
            '2015-10-27 00:00:00+03:00, 2018-10-27 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 1970-01-01 03:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-10 10:10:09+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-10 10:09:10+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-09 10:10:10+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-03 10:10:10+03:00',
            '1970-01-01 03:00:00+03:00, 2010-09-10 10:10:10+03:00',
            '1970-01-01 03:00:00+03:00, 2010-07-10 10:10:10+03:00',
            '1970-01-01 03:00:00+03:00, 2009-10-10 10:10:10+03:00',
            '1970-01-01 03:00:00+03:00, 2010-11-10 10:10:10+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-10 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-01-01 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-04 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-08 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-03 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-01 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-11-01 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-04 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-01 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-01-01 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-01 10:10:10+03:00',
            '1970-01-01 03:00:00+03:00, 2011-10-01 10:10:10+03:00',
            '1970-01-01 03:00:00+03:00, 2011-07-01 10:10:10+03:00',
            '1970-01-01 03:00:00+03:00, 2011-07-01 10:20:15+03:00',
            '1970-01-01 03:00:00+03:00, 2011-07-20 09:20:15+03:00',
            '1970-01-01 03:00:00+03:00, 2010-09-10 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2009-10-10 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2008-10-10 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2008-10-10 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2008-10-10 10:10:00+03:00',
            '1970-01-01 03:00:00+03:00, 2008-01-01 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2008-01-11 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2008-01-04 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2007-12-31 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2008-01-01 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2007-12-31 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2007-10-01 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2007-12-01 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2007-01-01 00:00:00+03:00',
        ],
    }

    # incorrect otl lines; undefined behavior
    otl_request_undefined = {
        'src': [
            '| otstats ... latest=abc',  # smth strange
            '| otstats ... latest=+abc-2m',  # smth strange, subtract 2 minutes
            '| otstats ... latest=1970-01-01 03:00:00',  # not correct datetime format
            '| otstats ... latest=1970-01-01-03:00:00',  # not correct datetime format
            '| otstats ... latest=now(',  # not correct format
            '| otstats ... earliest=1slatest=2d',  # not correct format
            '| otstats ... earliest=10/27/2018:00:00:00latest=10/27/2018:00:00:00',  # not correct format
        ],
        'dst': [
            '1970-01-01 03:00:00+03:00, 1970-01-01 03:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 2010-10-10 10:08:10+03:00',
            '1970-01-01 03:00:00+03:00, 1970-01-01 00:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 1970-01-01 03:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 1970-01-01 03:00:00+03:00',
            '2010-10-12 10:10:10+03:00, 1970-01-01 03:00:00+03:00',
            '1970-01-01 03:00:00+03:00, 1970-01-01 03:00:00+03:00',
        ],
    }

    # add error otl lines!

    def _extract_time(self, otl_line: str):
        _, time1, time2 = self.OTLTR.extract_timerange(otl_line, 0, 0)
        res = f"{datetime.fromtimestamp(time1, self.tz)}, " \
              f"{datetime.fromtimestamp(time2, self.tz)}"
        return res

    def _get_all_res(self):
        otl = self.otl_request_undefined
        for otl_line, expected in zip(otl['src'], otl['dst']):
            res = self._extract_time(otl_line)
            print(res)

    def test_valid_otl_line(self):
        otl = self.otl_request_correct
        for otl_line, expected in zip(otl['src'], otl['dst']):
            with self.subTest(line=otl_line):
                res = self._extract_time(otl_line)
                self.assertEqual(res, expected)

    def test_invalid_otl_line(self):
        otl = self.otl_request_undefined
        for otl_line, expected in zip(otl['src'], otl['dst']):
            with self.subTest(line=otl_line):
                res = self._extract_time(otl_line)
                self.assertEqual(res, expected)


def main():
    TestOTLTimeRangeExtractor()


if __name__ == "__main__":
    main()
