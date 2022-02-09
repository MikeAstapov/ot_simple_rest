import re
from datetime import timedelta
from datetime import datetime
from datetime import date as date_class
from dateutil.relativedelta import relativedelta


def datatime_reset_weekday(today: datetime) -> datetime:
    return today - timedelta(days=today.weekday())


def get_quarter(p_date: date_class) -> int:
    return (p_date.month - 1) // 3


def get_first_day_of_the_quarter(p_date: date_class) -> datetime:
    return datetime(p_date.year, 3 * get_quarter(p_date) + 1, 1)


def datetime_reset_quarter(p_date: datetime) -> datetime:
    return get_first_day_of_the_quarter(p_date.date())


class SplunkRelativeTimeModifier:
    abbreviations = {
        'S': ('s', 'sec', 'secs', 'second', 'seconds'),
        'M': ('m', 'min', 'minute', 'minutes'),
        'H': ('h', 'hr', 'hrs', 'hour', 'hours'),
        'd': ('d', 'day', 'days'),
        'w': ('w', 'week', 'weeks'),
        'm': ('mon', 'month', 'months'),
        'q': ('q', 'qtr', 'qtrs', 'quarter', 'quarters'),
        'Y': ('y', 'yr', 'yrs', 'year', 'years')
    }

    abbreviation_rules = {
        # calendar time range
        'std_time_range':
            {
                'S': {'allow_snap': True, 'snap_value': 0},
                'M': {'allow_snap': True, 'snap_value': 0},
                'H': {'allow_snap': True, 'snap_value': 0},
                'd': {'allow_snap': True, 'snap_value': 1},
                'm': {'allow_snap': True, 'snap_value': 1},
                'Y': {'allow_snap': False, 'snap_value': 0}},
        # fictitious time range
        'extra_time_range':
            {
                'w': {
                    'allow_snap': True, 'snap_value': None, 'level': 'd', 'factor_level': 7,
                    'func_reset_time': datatime_reset_weekday, 'value_range': [0, 7],
                },
                'q': {
                    'allow_snap': True, 'snap_value': None, 'level': 'm', 'factor_level': 3,
                    'func_reset_time': datetime_reset_quarter, 'value_range': [1, 3],
                }
            }
    }

    spliter_regex = r"(\+|-|\@)"
    abbr_regex = r"(\d+)"

    def __init__(self, current_datetime=datetime.now()):
        """
        Args:
            current_datetime: datetime relative to which to consider the shift
        """
        self.current_datetime = current_datetime
        self.res_datetime = current_datetime

    def _get_time_range_key_name_by_abbr(self, abbr: str, with_s: bool = False) -> (str, str) or (None, None):
        res_ = [
            (key, self.abbreviations[key][-1 if with_s else -2])
            for key in self.abbreviations if
            abbr in self.abbreviations[key]
        ]
        return res_[0] if res_ else (None, None)

    def _get_time_range_key_name_by_key(self, abbr_key: str, with_s: bool = False) -> (str, str) or (None, None):
        return self.abbreviations[abbr_key][-1 if with_s else -2] if abbr_key in self.abbreviations else (None, None)

    def _get_time_range_keys_by_key_under_curr_level(self, abbr_key: str, std_only: bool = True) -> list or None:
        """
        Return all time range level keys under current level.
        """
        res_ = []
        for key in self.abbreviations:
            if (std_only and key in self.abbreviation_rules['std_time_range']) or not std_only:
                res_ += [key]
            if key == abbr_key:
                break
        return res_[:-1] if res_ else None

    def _replace_time_range_levels_under_curr_to_zero_by_key(self, now: datetime, abbr_key: str) -> datetime:
        """
        get keys under current level; example: level - day/week, list - [sec, min, hour];
        than replace datetime ranges to zero; example: (hour=0, min=0, sec=0)
        """
        # get keys under current level; example: level - day, list - [sec, min]
        abbr_key_list = self._get_time_range_keys_by_key_under_curr_level(abbr_key, std_only=True)
        # create dict abbr_name and zero value from abbr_key_list with and check allow_snap
        snap_dict = {
            self._get_time_range_key_name_by_key(abbr_key_i, with_s=False):
                self.abbreviation_rules['std_time_range'][abbr_key_i]['snap_value']
            for abbr_key_i in abbr_key_list
            if self.abbreviation_rules['std_time_range'][abbr_key_i]['allow_snap']
        }
        return now.replace(**snap_dict)

    def _get_delta_shift_expression_elem(self, sign: int, abbr: str, value: int) -> relativedelta or None:
        delta = None

        # check abbreviations and calc delta shift
        time_range_key, abbr_full = self._get_time_range_key_name_by_abbr(abbr, with_s=True)

        # if standard time range
        if abbr_full and time_range_key in self.abbreviation_rules['std_time_range']:
            delta = relativedelta(**{abbr_full: sign * value})
        # if nonstandard time range
        elif abbr_full and time_range_key in self.abbreviation_rules['extra_time_range']:
            factor_level = self.abbreviation_rules['extra_time_range'][time_range_key]['factor_level']
            abbr_full_ = self._get_time_range_key_name_by_key(
                self.abbreviation_rules['extra_time_range'][time_range_key]['level'],
                with_s=True
            )
            if factor_level and abbr_full_:
                value *= factor_level
                delta = relativedelta(**{abbr_full_: sign * value})

        return delta

    def _get_delta_snap_expression_elem(self, num_abbr_union: list, abbr_key: str) -> relativedelta or None:
        def check_exists_value_and_value_range():
            return len(num_abbr_union) == 2 and num_abbr_union[1].isdigit() and \
                self.abbreviation_rules['extra_time_range'][abbr_key]['value_range']

        delta = None

        if check_exists_value_and_value_range():
            value = int(num_abbr_union[1])
            value_min, value_max = self.abbreviation_rules['extra_time_range'][abbr_key]['value_range']

            # check value range, min and max possible value; example
            if value_min <= value <= value_max:
                delta = relativedelta(
                    **{
                        self._get_time_range_key_name_by_key(
                            self.abbreviation_rules['extra_time_range'][abbr_key]['level'],
                            with_s=True
                        ):
                            value - 1
                    })
        return delta

    def _update_datetime_with_shift(self, num_abbr_union: list, sign: int):
        """
        Args:
            num_abbr_union: splitted expression element on nums and abbreviations
            sign: action for expression elements: + or -
        """
        value, abbr, delta = 1, None, None  # default values

        # check each element in split parts
        for union_elem in num_abbr_union:

            if union_elem.isdigit():
                value = int(union_elem)
            else:
                abbr = union_elem

            # calculate delta shift for union_elem
            delta = self._get_delta_shift_expression_elem(sign, abbr, value)

            # update
            if delta:
                self.res_datetime += delta
                value, abbr, delta = 1, None, None

    def _update_datetime_with_snap(self, num_abbr_union: list):
        """
        Args:
            num_abbr_union: splitted expression element on abbreviations and nums
        """

        def check_standard_time_range():
            return abbr_key in self.abbreviation_rules['std_time_range']

        def check_nonstandard_time_range():
            return abbr_key in self.abbreviation_rules['extra_time_range'] and \
                   self.abbreviation_rules['extra_time_range'][abbr_key]['func_reset_time']

        # FIRST PARAM
        if not num_abbr_union[0].isdigit():
            abbr = num_abbr_union[0]
            # check abbreviations and calc delta shift
            abbr_key, abbr_full = self._get_time_range_key_name_by_abbr(abbr, with_s=True)

            # if standard time range
            if check_standard_time_range():

                # get keys under current level; example: level - day/week, list - [sec, min, hour]
                # replace datetime ranges to zero; example: (hour=0, min=0, sec=0)
                self.res_datetime = self._replace_time_range_levels_under_curr_to_zero_by_key(self.res_datetime,
                                                                                              abbr_key)

            # if nonstandard time range and can reset datetime
            elif check_nonstandard_time_range():

                # reset curr level time range
                self.res_datetime = self.abbreviation_rules['extra_time_range'][abbr_key]['func_reset_time'](
                    self.res_datetime)

                # get keys under current level; example: level - day/week, list - [sec, min, hour]
                # replace datetime ranges to zero; example: (hour=0, min=0, sec=0)
                self.res_datetime = self._replace_time_range_levels_under_curr_to_zero_by_key(self.res_datetime,
                                                                                              abbr_key)

                # SECOND PARAM; value only for nonstandard time range or None if not in range
                delta = self._get_delta_snap_expression_elem(num_abbr_union, abbr_key)

                if delta:
                    self.res_datetime += delta

    def _split_expression_elem_on_num_abbr_union(self, expression_elem: str, sign: int):
        """
        Args:
            expression_elem: expression element
            sign: action for expression elements: +, - or @
        """

        # split expression_elem on nums and words
        num_abbr_union = list(filter(None, re.split(self.abbr_regex, expression_elem)))

        # time shift if + or - and not empty expression_elem
        if sign and num_abbr_union:
            self._update_datetime_with_shift(num_abbr_union, sign)

        # time snap if @
        elif not sign and 1 <= len(num_abbr_union) <= 2:
            self._update_datetime_with_snap(num_abbr_union)

    def process_timeline(self, expression_line: str) -> int:
        """
        Args:
            expression_line: string with time expression, example: -1mon@q+1d

        Returns:
            integer timestamp
        """
        # reset result datetime
        self.res_datetime = self.current_datetime

        # + = 1, - = -1, @ = 0
        sign: int = 1

        # split with +, -, @ on simple expressions
        for expression_elem in filter(None, re.split(self.spliter_regex, expression_line)):

            # check if spliter is +, -, @
            if re.findall(self.spliter_regex, expression_elem):
                sign = 1 if expression_elem == '+' else -1 if expression_elem == '-' else 0
            # else process expression_elem and change res_datetime
            else:
                self._split_expression_elem_on_num_abbr_union(expression_elem, sign)

        return int(self.res_datetime.timestamp())


class ProcessTimeline(SplunkRelativeTimeModifier):
    # Now available only one format: "%m/%d/%Y:%H:%M:%S".
    datetime_format: str = "%m/%d/%Y:%H:%M:%S"

    def __init__(self, current_datetime=datetime.now()):
        """
        Args:
            current_datetime: current time or None to use datetime.now()
        """
        super().__init__(current_datetime)
        self.current_datetime: datetime = current_datetime

    def _validate_strptime_format(self, timeline) -> bool:
        try:
            datetime.strptime(timeline, self.datetime_format)
        except ValueError:
            return False
        return True

    def select_category_and_process_timeline(self, timeline: str) -> int:
        """
        Selects one of 4 types of time recording and time offsets

        Args:
            timeline: argument value from otl request

        Returns:
            integer timestamp
        """

        # CATEGORY: "now()" or "now"
        if timeline in ("now", "now()"):
            return int(self.current_datetime.timestamp())

        # CATEGORY: timestamp
        if timeline.isdigit():
            return int(timeline)

        # CATEGORY: datetime, example: 10/27/2018:00:00:00
        if self._validate_strptime_format(timeline):
            return int(datetime.strptime(timeline, self.datetime_format).timestamp())

        # CATEGORY: splunk expression, example: -1h+2m@w2
        return self.process_timeline(timeline)


class OtlTimeRange(ProcessTimeline):

    def __init__(self, current_datetime=datetime.now()):
        """
        Class remove time ranges from otl line and convert them to timestamp format.

        Args:
            current_datetime: optional, set different time or default `None` set current time
        """
        super().__init__(current_datetime)
        self.current_datetime: datetime = current_datetime

    def clean_otl_and_process_timerange(self, otl_line: str, tws: int, twf: int) -> (str, int, int):
        """
        Args:
            otl_line: otl request line with time range
            tws: earliest timestamp
            twf: latest timestamp

        Returns:
            clean otl request, earliest time, latest time
        """

        dict_time_mod_args = {"earliest": 0, "latest": 0}
        # -> (earliest|latest)=([a-zA-Z0-9_*-\@]+)
        otl_line_regex = rf"({'|'.join(dict_time_mod_args.keys())})=\"?([()a-zA-Z0-9_*-\@]+)"

        otl_splited = dict(re.findall(otl_line_regex, otl_line))  # example: {'earliest': '1', 'latest': 'now()'}
        otl_cleaned = re.sub(otl_line_regex, "", otl_line)

        # check if no modifiers
        if not otl_splited:
            # print('empty')
            return otl_cleaned, tws, twf

        # process timeline
        for key, val in otl_splited.items():
            dict_time_mod_args[key]: int = self.select_category_and_process_timeline(val)

        # check if modifier_name in otl line and twf > tws then replace tws and twf
        if otl_splited and not (dict_time_mod_args["earliest"] != 0 and dict_time_mod_args["latest"] != 0 and
                                dict_time_mod_args["latest"] < dict_time_mod_args["earliest"]):
            tws, twf = dict_time_mod_args["earliest"], dict_time_mod_args["latest"]

        # print('Success')
        return otl_cleaned, tws, twf


# local tests
if __name__ == "__main__":

    current_time = datetime(2010, 10, 10, 10, 10, 10)
    OTLTR = OtlTimeRange(current_time)

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
            _, time1, time2 = OTLTR.clean_otl_and_process_timerange(req, 0, 0)
            res = f"{datetime.fromtimestamp(time1)}, {datetime.fromtimestamp(time2)}"
            # print(f"{datetime.fromtimestamp(time1)}, {datetime.fromtimestamp(time2)}")
            assert res == expected, \
                AssertionError(f"Got: {res}; Expected: {expected}")
        print("Success")
