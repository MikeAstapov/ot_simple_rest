import re
from datetime import datetime
from utils.time_parsers import NowParser, EpochParser, FormattedParser, SplunkModifiersParser


class TotalTimeParser:

    PROCESSORS = (
        EpochParser, NowParser, FormattedParser, SplunkModifiersParser  # Order matters!
    )

    @classmethod
    def _time_modify(cls, item: datetime):
        """Modify datetime before return. Customize here!"""
        return int(item.timestamp())

    @classmethod
    def parse(cls, time_string: str) -> datetime or None:
        """Apply all the processors before parsing success"""
        for parser in cls.PROCESSORS:
            parsed_time = parser().parse(time_string)
            if parsed_time:
                return cls._time_modify(parsed_time)


class OTLTimeRangeExtractor:

    FIELDS = ("earliest", "latest")

    # List of time parsing processors. Will be user one by one trying to extract datetime from the arg string
    PARSER = TotalTimeParser

    @classmethod
    def _timed_args_are_consistent(cls, args: dict) -> bool:
        """Check extracted and parsed args. Customize here!"""
        if not set(args).issubset(set(cls.FIELDS)):
            return False
        # TODO check necessity
        # if args.get(cls.FIELDS[0], 0) > args.get(cls.FIELDS[-1], 0):
        #     return False
        return True

    def __init__(self, current_time: datetime = datetime.now()):
        self.current_time = current_time

    def _split_otl(self, line: str) -> (str, dict):
        """ Split OTL line by regex and extract timed args"""
        # -> (earliest|latest)=([a-zA-Z0-9_*-\@]+)
        otl_line_regex = rf"({'|'.join(self.FIELDS)})=\"?([()a-zA-Z0-9_*-\@]+)"
        timed_args = dict(re.findall(otl_line_regex, line))  # example: {'earliest': '1', 'latest': 'now()'}
        otl_cleaned = re.sub(otl_line_regex, "", line)
        return otl_cleaned, timed_args

    def _parse_arg(self, arg: str) -> int or None:
        """Call the external parser"""
        return self.PARSER.parse(arg)

    def extract_timerange(self, otl_line: str, tws: int, twf: int) -> (str, int, int):
        """
        Args:
            otl_line: otl request line with time range
            tws: earliest timestamp
            twf: latest timestamp

        Returns:
            clean otl request, earliest time, latest time
        """

        otl_cleaned, timed_args = self._split_otl(otl_line)
        print(otl_cleaned, timed_args)

        # check if no modifiers
        if not timed_args:
            return otl_cleaned, tws, twf

        # process timeline
        for key, arg in timed_args.items():
            timed_args[key] = self._parse_arg(arg)

        # check if modifier_name in otl line and twf > tws then replace tws and twf
        if self._timed_args_are_consistent(timed_args):
            tws, twf = timed_args.get(self.FIELDS[0], tws), timed_args.get(self.FIELDS[-1], twf)

        return otl_cleaned, tws, twf


# local tests
if __name__ == "__main__":

    current_time = datetime(2010, 10, 10, 10, 10, 10)
    OTLTR = OTLTimeRangeExtractor(current_time)

    otl_request_correct = {
        'src': [
            '| otstats abrakadabra latest=now()',  # check now()
            '| otstats ... latest=now',  # check now
            '| otstats ... earliest=1111111100 latest=1111111111',  # check earliest < latest and timestamp
            '| otstats ... earliest=10/27/2015:00:00:00 latest="10/27/2018:00:00:00"',  # check earliest < latest and
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
            '1970-01-01 03:00:00, 2010-10-10 10:10:10',
            '1970-01-01 03:00:00, 2010-10-10 10:10:10',
            '2005-03-18 04:58:20, 2005-03-18 04:58:31',
            '2015-10-27 00:00:00, 2018-10-27 00:00:00',
            '1970-01-01 03:00:00, 1970-01-01 03:00:00',
            '1970-01-01 03:00:00, 2010-10-10 10:10:09',
            '1970-01-01 03:00:00, 2010-10-10 10:09:10',
            '1970-01-01 03:00:00, 2010-10-09 10:10:10',
            '1970-01-01 03:00:00, 2010-10-03 10:10:10',
            '1970-01-01 03:00:00, 2010-09-10 10:10:10',
            '1970-01-01 03:00:00, 2010-07-10 10:10:10',
            '1970-01-01 03:00:00, 2009-10-10 10:10:10',
            '1970-01-01 03:00:00, 2010-11-10 10:10:10',
            '1970-01-01 03:00:00, 2010-10-10 00:00:00',
            '1970-01-01 03:00:00, 2010-01-01 00:00:00',
            '1970-01-01 03:00:00, 2010-10-04 00:00:00',
            '1970-01-01 03:00:00, 2010-10-08 00:00:00',
            '1970-01-01 03:00:00, 2010-10-03 00:00:00',
            '1970-01-01 03:00:00, 2010-10-01 00:00:00',
            '1970-01-01 03:00:00, 2010-11-01 00:00:00',
            '1970-01-01 03:00:00, 2010-10-04 00:00:00',
            '1970-01-01 03:00:00, 2010-10-01 00:00:00',
            '1970-01-01 03:00:00, 2010-01-01 00:00:00',
            '1970-01-01 03:00:00, 2010-10-01 10:10:10',
            '1970-01-01 03:00:00, 2011-10-01 10:10:10',
            '1970-01-01 03:00:00, 2011-07-01 10:10:10',
            '1970-01-01 03:00:00, 2011-07-01 10:20:15',
            '1970-01-01 03:00:00, 2011-07-20 09:20:15',
            '1970-01-01 03:00:00, 2010-09-10 00:00:00',
            '1970-01-01 03:00:00, 2009-10-10 00:00:00',
            '1970-01-01 03:00:00, 2008-10-10 00:00:00',
            '1970-01-01 03:00:00, 2008-10-10 00:00:00',
            '1970-01-01 03:00:00, 2008-10-10 10:10:00',
            '1970-01-01 03:00:00, 2008-01-01 00:00:00',
            '1970-01-01 03:00:00, 2008-01-11 00:00:00',
            '1970-01-01 03:00:00, 2008-01-04 00:00:00',
            '1970-01-01 03:00:00, 2007-12-31 00:00:00',
            '1970-01-01 03:00:00, 2008-01-01 00:00:00',
            '1970-01-01 03:00:00, 2007-12-31 00:00:00',
            '1970-01-01 03:00:00, 2007-10-01 00:00:00',
            '1970-01-01 03:00:00, 2007-12-01 00:00:00',
            '1970-01-01 03:00:00, 2007-01-01 00:00:00',
        ],
    }

    # undefined
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
            '1970-01-01 03:00:00, 2010-10-10 10:10:10',
            '1970-01-01 03:00:00, 2010-10-10 10:08:10',
            '1970-01-01 03:00:00, 2010-10-10 10:10:10',
            '1970-01-01 03:00:00, 2010-10-10 10:10:10',
            '1970-01-01 03:00:00, 2010-10-10 10:10:10',
            '2010-10-12 10:10:10, 1970-01-01 03:00:00',
            '2010-10-10 10:10:10, 1970-01-01 03:00:00',
        ],
    }

    for i, otl in enumerate((otl_request_correct, otl_request_undefined)):
        print(f"Part {i + 1}")
        for req, expected in zip(otl['src'], otl['dst']):
            _, time1, time2 = OTLTR.extract_timerange(req, 0, 0)
            res = f"{datetime.fromtimestamp(time1)}, {datetime.fromtimestamp(time2)}"
            # print(f"{datetime.fromtimestamp(time1)}, {datetime.fromtimestamp(time2)}")
            assert res == expected, \
                AssertionError(f"Got: {res}; Expected: {expected}")
        print("Success")
