import re
from datetime import timedelta
from datetime import datetime
from datetime import date as date_class
from dateutil.relativedelta import relativedelta


""" !!!!!!!!!!! """
""" OLD VERSION """
""" !!!!!!!!!!! """


# class Timerange:
#     @staticmethod
#     def get_timestamp(time):
#         if time == "now":
#             return int(datetime.now().timestamp())
#         regex = r"(-|\+|^)?(\d+)(s|m|h|d|w|M|y)?"
#         result = re.match(regex, time)
#         if result is not None:
#             diff_num = int(result.group(2))
#             if not result.group(1) and not result.group(3):
#                 return diff_num
#
#             dict_delta = {
#                 's': timedelta(seconds=diff_num),
#                 'm': timedelta(minutes=diff_num),
#                 'h': timedelta(hours=diff_num),
#                 'd': timedelta(days=diff_num),
#                 'w': timedelta(weeks=diff_num),
#                 'M': timedelta(weeks=4*diff_num),
#                 'y': timedelta(weeks=52*diff_num)
#                          }
#             now = datetime.now()
#             delta = dict_delta[result.group(3)]
#             if result.group(1) == "-":
#                 res_time = now - delta
#             else:
#                 res_time = now + delta
#             return int(res_time.timestamp())
#         return None
#
#     @staticmethod
#     def removetime(otl, tws, twf):
#         _tws = tws
#         _twf = twf
#         regex = r"(earliest|latest)=([a-zA-Z0-9_*-]+)"
#         for (time_modifier, time) in re.findall(regex, otl):
#             if time_modifier == "earliest":
#                 _tws = Timerange.get_timestamp(time)
#             if time_modifier == "latest":
#                 _twf = Timerange.get_timestamp(time)
#         service_otl = re.sub(regex, "", otl)
#         return service_otl, _tws, _twf


""" !!!!!!!!!!! """
""" NEW VERSION """
""" !!!!!!!!!!! """


class Timerange:
    """
    Cool class!

    Features:
        - add format selection from outside
        - add auto detecting datetime format
    """

    datetime_format: str = "%m/%d/%Y:%H:%M:%S"

    def __init__(self, current_datetime: datetime = None):
        """Class remove time ranges from otl line and convert them to timestamp format.

        Args:
            current_datetime: optional, set different time or default `None` set current time

        """
        self.__now: datetime = current_datetime if current_datetime else datetime.now()

    def __get_current_time(self) -> int:
        """

        Returns:
            Current time in timestamp format.

        """
        return int(self.__now.timestamp())

    @staticmethod
    def __transform_timestamp_str2int(timestamp: str) -> int:
        """
        Transform string timestamp to integer.

        Args:
            timestamp: string timestamp

        Returns:
            Integer timestamp.

        """
        return int(timestamp)

    def __transform_datetime_format(self, datetime_line: str) -> int:
        """
        Transform datetime to timestamp. Now avaliable only one format: "%m/%d/%Y:%H:%M:%S".

        Args:
            datetime_line: line with datetime format, example: 10/27/2018:00:00:00

        Returns:
            datetime in timestamp format

        """
        return int(datetime.strptime(datetime_line, self.datetime_format).timestamp())

    def __transform_splunk_abbreviations(self, expression_line: str) -> int:
        """
        Transform splunk expression to timestamp.

        Args:
            expression_line: line with splunk expression, example: -mon@q+1d

        Returns:
            integer timestamp

        """

        # copy original time
        now: datetime = self.__now

        """ !!!!!!!!!!!!!!!! """
        """ CONFIG FUNCTIONS """
        """ !!!!!!!!!!!!!!!! """

        def datatime_reset_weekday(today: datetime) -> datetime:
            return today - timedelta(days=today.weekday())

        def datetime_reset_quarter(today: datetime) -> datetime:
            def get_first_day_of_the_quarter(p_date: date_class) -> datetime:
                def get_quarter(p_date_: date_class) -> int:
                    return (p_date_.month - 1) // 3
                return datetime(p_date.year, 3 * get_quarter(p_date) + 1, 1)
            return get_first_day_of_the_quarter(today.date())

        """ !!!!!!!!!!!!! """
        """ CONFIG PARAMS """
        """ !!!!!!!!!!!!! """

        abbreviations: dict = {
            'S': ['s', 'sec', 'secs', 'second', 'seconds'],
            'M': ['m', 'min', 'minute', 'minutes'],
            'H': ['h', 'hr', 'hrs', 'hour', 'hours'],
            'd': ['d', 'day', 'days'],
            'w': ['w', 'week', 'weeks'],
            'm': ['mon', 'month', 'months'],
            'q': ['q', 'qtr', 'qtrs', 'quarter', 'quarters'],
            'Y': ['y', 'yr', 'yrs', 'year', 'years']
        }

        abbreviations_rules: dict = {
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

        """ !!!!!!!!!!!!!!!!!!!! """
        """ ADDITIONAL FUNCTIONS """
        """ !!!!!!!!!!!!!!!!!!!! """

        def get_full_abbr_by_name(abbr_: str, with_s: bool = False) -> (str, str) or (None, None):
            res_ = [(key, abbreviations[key][-1 if with_s else -2])
                    for key in abbreviations if
                    abbr_ in abbreviations[key]]
            return res_[0] if res_ else (None, None)

        def get_full_abbr_by_key(abbr_key_: str, with_s: bool = False) -> (str, str) or (None, None):
            return abbreviations[abbr_key_][-1 if with_s else -2] if abbr_key_ in abbreviations else (None, None)

        def get_abrr_key_list_up2level(abbr_key_: str, std_only: bool = True) -> list or None:
            """
            Return all time range levels under current.
            """
            res_ = []
            for key in abbreviations:
                if (std_only and key in abbreviations_rules['std_time_range']) or not std_only:
                    res_ += [key]
                if key == abbr_key_:
                    break
            return res_[:-1] if res_ else None

        def replace_datetime_range_levels_to_zero_under_current(now_: datetime, abbr_key_: str) -> datetime:
            """
            get keys under current level; example: level - day/week, list - [sec, min, hour];
            than replace datetime ranges to zero; example: (hour=0, min=0, sec=0)
            """
            # get keys under current level; example: level - day, list - [sec, min]
            abbr_key_list = get_abrr_key_list_up2level(abbr_key_, std_only=True)
            # create dict abbr_name and zero value from abbr_key_list with and check allow snap
            snap_dict = {
                get_full_abbr_by_key(abbr_key_i, with_s=False):
                    abbreviations_rules['std_time_range'][abbr_key_i]['snap_value']
                for abbr_key_i in abbr_key_list
                if abbreviations_rules['std_time_range'][abbr_key_i]['allow_snap']}
            return now_.replace(**snap_dict)

        """ !!!!!!!!!!!!!! """
        """ ALGORITHM BODY """
        """ !!!!!!!!!!!!!! """

        spliter_regex: str = r"(\+|-|\@)"
        abbr_regex: str = r"(\d+)"
        sign: int = 1  # + = 1, - = -1, @ = 0

        # split with +, -, @
        timeline_splitted: list = [elem for elem in re.split(spliter_regex, expression_line) if elem]

        for timiline_elem in timeline_splitted:

            # check if spliter is +, -, @
            if re.findall(spliter_regex, timiline_elem):
                sign = 1 if timiline_elem is '+' else -1 if timiline_elem is '-' else 0
                continue

            # split timiline_elem on nums and words
            num_abbr_union: list = list(filter(None, re.split(abbr_regex, timiline_elem)))

            # time shift if + or -
            if sign and 0 < len(num_abbr_union):
                value, abbr = 1, None  # default values

                # check each element in split parts
                for elem_union in num_abbr_union:

                    if elem_union.isdigit():
                        value = int(elem_union)
                    else:
                        abbr = elem_union

                    # check abbreviations and calc delta shift
                    abbr_key, abbr_full = get_full_abbr_by_name(abbr, with_s=True)

                    # if standard time range
                    if abbr_full and abbr_key in abbreviations_rules['std_time_range']:
                        now += relativedelta(**{abbr_full: sign * value})
                        value, abbr = 1, None

                    # if nonstandard time range
                    elif abbr_full and abbr_key in abbreviations_rules['extra_time_range']:
                        factor_level = abbreviations_rules['extra_time_range'][abbr_key]['factor_level']
                        abbr_full = get_full_abbr_by_key(abbreviations_rules['extra_time_range'][abbr_key]['level'],
                                                         with_s=True)
                        if factor_level and abbr_full:
                            value *= factor_level
                            now += relativedelta(**{abbr_full: sign * value})
                        value, abbr = 1, None

            # time snap if @
            # тут без говнокода никак, уж извините
            elif not sign and 1 <= len(num_abbr_union) <= 2:

                # FIRST PARAM
                if not num_abbr_union[0].isdigit():
                    abbr = num_abbr_union[0]
                    # check abbreviations and calc delta shift
                    abbr_key, abbr_full = get_full_abbr_by_name(abbr, with_s=True)

                    # if standard time range
                    if abbr_key in abbreviations_rules['std_time_range']:

                        # get keys under current level; example: level - day/week, list - [sec, min, hour]
                        # replace datetime ranges to zero; example: (hour=0, min=0, sec=0)
                        now = replace_datetime_range_levels_to_zero_under_current(now, abbr_key)

                    # if nonstandard time range and can reset datetime
                    elif abbr_key in abbreviations_rules['extra_time_range'] and \
                            abbreviations_rules['extra_time_range'][abbr_key]['func_reset_time']:

                        # reset curr level time range
                        now = abbreviations_rules['extra_time_range'][abbr_key]['func_reset_time'](now)

                        # get keys under current level; example: level - day/week, list - [sec, min, hour]
                        # replace datetime ranges to zero; example: (hour=0, min=0, sec=0)
                        now = replace_datetime_range_levels_to_zero_under_current(now, abbr_key)

                        # SECOND PARAM; value for nonstandard time range if value range exists
                        if len(num_abbr_union) == 2 and num_abbr_union[1].isdigit() and \
                                abbreviations_rules['extra_time_range'][abbr_key]['value_range']:
                            value = int(num_abbr_union[1])
                            value_min, value_max = abbreviations_rules['extra_time_range'][abbr_key]['value_range']

                            # check value range, min and max possible value; example
                            if value_min <= value <= value_max:
                                now += relativedelta(
                                    **{
                                        get_full_abbr_by_key(
                                            abbreviations_rules['extra_time_range'][abbr_key]['level'],
                                            with_s=True
                                        ):
                                            value - 1
                                    })

        return int(now.timestamp())

    def select_category_and_process(self, timeline: str) -> int:
        """
        Selects one of 4 types of time recording and time offsets

        Args:
            timeline: argument value from otl request

        Returns:
            integer timestamp

        """

        def validate_strptime_format(datetime_line: str) -> bool:
            try:
                datetime.strptime(datetime_line, self.datetime_format)
            except ValueError:
                return False
            return True

        # "now()" or "now"
        if timeline is "now()" or timeline is "now":
            return self.__get_current_time()

        # timestamp
        if timeline.isdigit():
            return self.__transform_timestamp_str2int(timeline)

        # datetime, example: 10/27/2018:00:00:00
        if validate_strptime_format(timeline):
            return self.__transform_datetime_format(timeline)

        return self.__transform_splunk_abbreviations(timeline)

    # main function
    def otl_remove_timerange(self, otl_line: str, tws: int, twf: int) -> (str, int, int):
        """

        Args:
            otl_line: otl request line with time range
            tws: earliest timestamp
            twf: latest timestamp

        Returns:
            otl request, earliest time, latest time

        """

        dict_time_mod_args: dict = {"earliest": 0, "latest": 0}
        # -> (earliest|latest)=([a-zA-Z0-9_*-\@]+)
        otl_line_regex: str = rf"({'|'.join(dict_time_mod_args.keys())})=\"?([()a-zA-Z0-9_*-\@]+)"

        otl_splited: dict = dict(re.findall(otl_line_regex, otl_line))  # example: {'earliest': '1', 'latest': 'now()'}
        otl_cleaned: str = re.sub(otl_line_regex, "", otl_line)

        # check if no modifiers
        if not otl_splited:
            # print('empty')
            return otl_cleaned, tws, twf

        # process time
        for key, val in otl_splited.items():
            dict_time_mod_args[key]: int = self.select_category_and_process(val)

        # check if modifier_name in otl line and twf > tws then replace tws and twf
        if otl_splited and not (dict_time_mod_args["earliest"] != 0 and dict_time_mod_args["latest"] != 0 and
                                dict_time_mod_args["latest"] < dict_time_mod_args["earliest"]):
            tws, twf = dict_time_mod_args["earliest"], dict_time_mod_args["latest"]

        # print('Success')
        return otl_cleaned, tws, twf


# local tests

if __name__ == "__main__":

    current_time = datetime(2010, 10, 10, 10, 10, 10)
    MT = Timerange(current_time)

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
        print(f"Part {i+1}")
        for req, expected in zip(otl['src'], otl['dst']):
            _, time1, time2 = MT.otl_remove_timerange(req, 0, 0)
            res = f"{datetime.fromtimestamp(time1)}, {datetime.fromtimestamp(time2)}"
            # print(f"{datetime.fromtimestamp(time1)}, {datetime.fromtimestamp(time2)}")
            assert res == expected, \
                AssertionError(f"Got: {res}; Expected: {expected}")
        print("Success")
