from .base_loader import BaseLoader
import json
import os
from file_read_backwards import FileReadBackwards
from typing import List, Dict, Optional, Tuple


class TimelinesLoader(BaseLoader):

    """
    Main purpose is to load data from cid and return the data as a reversed list of dictionaries.
    For timelines we need not all data but only the data that enters timelines timeinterval
    multiplied by the number of objects on a timeline.
    Hence we are reading the data backwards to prevent reading useless data for the timeline.
    TimelinesBuilder already knows that the list is reversed, and it will reverse timeline after creating it.
    """

    def __init__(self, mem_conf: Dict, static_conf: Dict, biggest_interval: int):
        super().__init__(mem_conf, static_conf)
        self.BIGGEST_INTERVAL = biggest_interval

    def load_data(self, cid: str) -> List[Dict]:
        """
        Load data by cid
        :param cid:         OT_Dispatcher's job cid
        :return:            list of dicts from json lines
        """
        data = []
        last_time = None
        break_now = False
        path_to_cache_dir = self._get_path_to_cache_dir(cid)
        file_names = self._get_cache_file_names(path_to_cache_dir, cid)
        for file_name in file_names:
            if break_now:
                break
            self.logger.debug(f'Reading part: {file_name}')
            break_now, last_time = self.read_file_backwards(data, os.path.join(path_to_cache_dir, file_name), last_time)
        return data  # is not reversed intentionally. This way it is easier to build a timeline

    def read_file_backwards(self, data: List, data_path: str, last_time: Optional[int]) -> Tuple[bool, int]:
        """
        Reads file and adds it to data list
        :param data:        list of data that is mutated inside this method
        :param data_path:   path to file
        :param last_time:   left border of a time interval
        :return bool:       indicating whether it is time to stop reading files
        :return int:        changing left border of the interval
        """
        with FileReadBackwards(data_path) as fr:
            for line in fr:
                tmp = json.loads(line)
                if last_time:
                    if last_time - tmp['_time'] > self.BIGGEST_INTERVAL:
                        return True, last_time
                else:
                    last_time = tmp['_time']
                data.append(tmp)
        return False, last_time
