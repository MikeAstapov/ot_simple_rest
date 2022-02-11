import re
from datetime import datetime

from utils.time_parsers import NowParser, EpochParser, FormattedParser, SplunkModifiersParser, TimeParser


class TotalTimeParser(TimeParser):

    # ORDER MATTERS!  {Processor: (*args)}
    PROCESSORS = {
        EpochParser: (),
        NowParser: ('current_datetime',),
        SplunkModifiersParser: ('current_datetime',),
        FormattedParser: (),
    }

    def __init__(self, current_datetime: datetime = datetime.now(), datetime_format: str = "%m/%d/%Y:%H:%M:%S"):
        """
        Args:
            current_datetime: datetime relative to which to consider the shift
            datetime_format: date and time format, example: "%m/%d/%Y:%H:%M:%S"
        """
        super().__init__(current_datetime=current_datetime, datetime_format=datetime_format)
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

    def parse(self, time_string: str) -> int or None:
        """Apply all the processors before parsing success"""
        for parser, p_args in self.PROCESSORS.items():
            parsed_time = parser(**p_args).parse(time_string)
            if parsed_time:
                return self._time_modify(parsed_time)


class OTLTimeRangeExtractor:

    FIELDS = ("earliest", "latest")

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

    def __init__(self, current_datetime: datetime = datetime.now()):
        """
        Args:
            current_datetime: datetime relative to which to consider the shift
        """
        self.PARSER = self.PARSER(current_datetime=current_datetime)

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
