from datetime import datetime, timedelta, date
import re
from abc import abstractmethod

import dateutil.parser
from dateutil.relativedelta import relativedelta


def datatime_reset_weekday(today: datetime) -> datetime:
    return today - timedelta(days=today.weekday())


def get_quarter(p_date: date) -> int:
    return (p_date.month - 1) // 3


def get_first_day_of_the_quarter(p_date: date) -> datetime:
    return datetime(p_date.year, 3 * get_quarter(p_date) + 1, 1)


def datetime_reset_quarter(p_date: datetime) -> datetime:
    return get_first_day_of_the_quarter(p_date.date())


class TimeParser:

    def __init__(self, current_datetime: datetime = None, datetime_format: str = None):
        """
        Args:
            current_datetime: datetime relative to which to consider the shift
            datetime_format: datetime_format
        """
        self.current_datetime = current_datetime
        self.datetime_format = datetime_format

    @abstractmethod
    def parse(self, time_string: str) -> datetime or None:
        """Provide conversion method"""
        raise NotImplementedError


class NowParser(TimeParser):
    PATTERN = ('now', 'now()', 'current')

    def __init__(self, current_datetime: datetime = datetime.now()):
        super().__init__(current_datetime=current_datetime)

    def parse(self, time_string: str) -> datetime or None:
        return self.current_datetime if time_string.lower() in self.PATTERN else None


class EpochParser(TimeParser):

    def parse(self, time_string: str) -> datetime or None:
        if time_string.isdigit():
            try:
                return datetime.fromtimestamp(float(time_string))
            except ValueError:
                return


class FormattedParser(TimeParser):

    def parse(self, time_string: str) -> datetime or None:
        try:
            dateutil.parser.parserinfo.JUMP.append(':')
            return dateutil.parser.parser().parse(time_string)
        except (dateutil.parser.ParserError, OverflowError):
            return


class SplunkModifiersParser(TimeParser):

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

    def __init__(self, current_datetime: datetime = datetime.now()):
        """
        Args:
            current_datetime: datetime relative to which to consider the shift
        """
        super().__init__(current_datetime=current_datetime)
        self._res_datetime = None

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
        snap_dict.update(microsecond=0)
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
                self._res_datetime += delta
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
                self._res_datetime = self._replace_time_range_levels_under_curr_to_zero_by_key(self._res_datetime,
                                                                                               abbr_key)

            # if nonstandard time range and can reset datetime
            elif check_nonstandard_time_range():

                # reset curr level time range
                self._res_datetime = self.abbreviation_rules['extra_time_range'][abbr_key]['func_reset_time'](
                    self._res_datetime)

                # get keys under current level; example: level - day/week, list - [sec, min, hour]
                # replace datetime ranges to zero; example: (hour=0, min=0, sec=0)
                self._res_datetime = self._replace_time_range_levels_under_curr_to_zero_by_key(self._res_datetime,
                                                                                               abbr_key)

                # SECOND PARAM; value only for nonstandard time range or None if not in range
                delta = self._get_delta_snap_expression_elem(num_abbr_union, abbr_key)

                if delta:
                    self._res_datetime += delta

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

    def parse(self, time_string: str) -> datetime or None:
        """
        Args:
            time_string: string with time expression, example: -1mon@q+1d

        Returns:
            integer timestamp
        """
        # reset result datetime
        self._res_datetime = self.current_datetime

        # + = 1, - = -1, @ = 0
        sign = 1

        # split with +, -, @ on simple expressions
        for expression_elem in filter(None, re.split(self.spliter_regex, time_string)):

            # check if spliter is +, -, @
            if re.findall(self.spliter_regex, expression_elem):
                sign = 1 if expression_elem == '+' else -1 if expression_elem == '-' else 0
            # else process expression_elem and change res_datetime
            else:
                self._split_expression_elem_on_num_abbr_union(expression_elem, sign)

        return self._res_datetime if self._res_datetime != self.current_datetime else None
