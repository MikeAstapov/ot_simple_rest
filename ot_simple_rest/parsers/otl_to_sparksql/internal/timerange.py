import re
from datetime import datetime, timezone, timedelta
from typing import Optional

from utils.time_parsers import NowParser, EpochParser, FormattedParser, SplunkModifiersParser


class TotalTimeParser:
    """
    Tries to parse given string as a datetime object using sequence of processors.
    Returns datetime modified with _time_modify method.
    """

    # ORDER MATTERS!  {Processor: (*args)}
    PROCESSORS = {
        EpochParser: ('tz',),
        NowParser: ('current_datetime', 'tz',),
        SplunkModifiersParser: ('current_datetime', 'tz',),
        FormattedParser: ('current_datetime', 'tz',),
    }

    def __init__(self, current_datetime: datetime = datetime.now(), tz: Optional[timezone] = None):
        """
        Args:
            current_datetime: datetime relative to which to consider the shift
            tz: time zone
        """
        self._processor_args2kwargs(locals())

    def _processor_args2kwargs(self, locals_init: dict):
        self.PROCESSORS = {
            parser: {arg: locals_init[arg] for arg in p_args}
            for parser, p_args in self.PROCESSORS.items()
        }

    @staticmethod
    def _time_modify(item: datetime) -> int:
        """Modify datetime before return. Customize here!"""
        return int(item.timestamp())

    def parse(self, time_string: str) -> Optional[int]:
        """
        Apply all the processors before parsing success

        >>> TotalTimeParser(current_datetime=datetime.fromtimestamp(1234567890)).parse('12.02.1983')
        439160400
        >>> TotalTimeParser(current_datetime=datetime.fromtimestamp(1234567890)).parse('-27days@day')
        1232226000
        >>> TotalTimeParser(current_datetime=datetime(2010, 10, 10, 10, 10, 10,)).parse("-12mon1y@y")
        1199134800
        """
        for parser, p_args in self.PROCESSORS.items():
            parsed_time = parser(**p_args).parse(time_string)
            if parsed_time:
                return self._time_modify(parsed_time)


class OTLTimeRangeExtractor:

    FIELDS = ("earliest", "latest")
    ESCAPE_REGEX = r"[\"|']"
    BODY_REGEX = r"()a-zA-Z0-9_*-\@"
    OTL_REGEX = rf"({'|'.join(FIELDS)})=({ESCAPE_REGEX}+[{BODY_REGEX}\s]+{ESCAPE_REGEX}+|[{BODY_REGEX}]+)"

    # Time parsing processor.
    # Must implement "parse" method and return parsed and modified time or None if failed to extract.
    PARSER = TotalTimeParser

    @classmethod
    def _timed_args_are_consistent(cls, args: dict) -> bool:
        """Check extracted and parsed args. Customize here!"""
        if not set(args).issubset(set(cls.FIELDS)):
            return False
        # timestamp comparison
        if set(cls.FIELDS).issubset(set(args)) and args[cls.FIELDS[0]] > args[cls.FIELDS[1]]:
            return False
        return True

    @classmethod
    def _split_otl(cls, line: str) -> (str, dict):
        """
        Split OTL line by regex and extract timed args

        >>> OTLTimeRangeExtractor()._split_otl('| ... earliest="1234567890"')
        ('| ... ', {'earliest': '1234567890'})
        >>> OTLTimeRangeExtractor()._split_otl("| ... latest=1234567890")
        ('| ... ', {'latest': '1234567890'})
        >>> OTLTimeRangeExtractor()._split_otl("| ... earliest=\\"1970-01-01 06:00:00' latest=\\"1970-01-01 06:00:00'")
        ('| ...  ', {'earliest': '1970-01-01 06:00:00', 'latest': '1970-01-01 06:00:00'})
        >>> OTLTimeRangeExtractor()._split_otl("| ... earliest=''''123' latest=1234")
        ('| ...  ', {'earliest': '123', 'latest': '1234'})
        """
        # timed_args = dict(re.findall(self.OTL_REGEX, line))  # example: {'earliest': '1', 'latest': 'now()'}
        timed_args = {key: val.strip('"\'') for key, val in re.findall(cls.OTL_REGEX, line)}
        otl_cleaned = re.sub(cls.OTL_REGEX, "", line)
        return otl_cleaned, timed_args

    def __init__(self, current_datetime: datetime = datetime.now(), tz: Optional[timezone] = None):
        """
        Args:
            current_datetime: datetime relative to which to consider the shift
            tz: time zone
        """
        self.PARSER = self.PARSER(current_datetime=current_datetime, tz=tz)

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

        >>> OTLTimeRangeExtractor().extract_timerange("| eval salary=500000 latest=14.02.1983:15:00", 0, 0)
        ('| eval salary=500000 ', 0, 414072000)
        >>> OTLTimeRangeExtractor().extract_timerange('| otstats ... earliest=10/27/2015:00:00:00 latest="10/27/2018:00:00:00"', 0, 0)
        ('| otstats ...  ', 1445893200, 1540587600)
        >>> OTLTimeRangeExtractor(tz=timezone(timedelta(hours=3))).extract_timerange('| otstats ... latest="1970-01-01 03:00:00"', 0, 0)
        ('| otstats ... ', 0, 0)
        """

        otl_cleaned, timed_args = self._split_otl(otl_line)

        # check if no modifiers
        if not timed_args:
            return otl_cleaned, tws, twf

        # process timeline
        for key, arg in timed_args.items():
            timed_args[key] = self._parse_arg(arg)

        # check if modifier_name in otl line and twf > tws then replace tws and twf
        if self._timed_args_are_consistent(timed_args):
            tws, twf = timed_args.get(self.FIELDS[0], tws) or tws, timed_args.get(self.FIELDS[-1], twf) or twf

        return otl_cleaned, tws, twf


if __name__ == '__main__':
    import doctest
    doctest.testmod()
