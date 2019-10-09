import re
from datetime import timedelta
from datetime import datetime


class Timerange:
    @staticmethod
    def get_timestamp(time):
        if time == "now":
            return int(datetime.now().timestamp())
        regex = r"(-|\+|^)(\d+)(s|m|h|d|w|M|y)"
        result = re.match(regex, time)
        if result is not None:
            diff_num = int(result.group(2))
            dict_delta = {
                's': timedelta(seconds=diff_num),
                'm': timedelta(minutes=diff_num),
                'h': timedelta(hours=diff_num),
                'd': timedelta(days=diff_num),
                'w': timedelta(weeks=diff_num),
                'M': timedelta(weeks=4*diff_num),
                'y': timedelta(weeks=52*diff_num)
                         }
            now = datetime.now()
            delta = dict_delta[result.group(3)]
            if result.group(1) == "-":
                res_time = now - delta
            else:
                res_time = now + delta
            return int(res_time.timestamp())
        return None

    @staticmethod
    def removetime(spl, tws, twf):
        _tws = tws
        _twf = twf
        regex = r"(earliest|latest)=([a-zA-Z0-9_*-]+)"
        for (time_modifier, time) in re.findall(regex, spl):
            if (time_modifier == "earliest"):
                _tws = Timerange.get_timestamp(time)
            if (time_modifier == "latest"):
                _twf = Timerange.get_timestamp(time)
        service_spl = re.sub(regex, "", spl)
        return (service_spl, _tws, _twf)
