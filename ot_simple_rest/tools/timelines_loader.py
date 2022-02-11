from .base_loader import BaseLoader
from pathlib import Path
import json
import os
from file_read_backwards import FileReadBackwards
from tornado.web import HTTPError


class TimelinesLoader(BaseLoader):

    """
    main purpose to load data from cid and return the data as a reversed list of dictionaries
    """

    def __init__(self, mem_conf, static_conf, biggest_interval):
        super().__init__(mem_conf, static_conf)
        self.BIGGEST_INTERVAL = biggest_interval

    def load_data(self, cid):
        """
        Load data by cid

        :param cid:         OT_Dispatcher's job cid
        :return:            list of dicts from json lines
        """
        data = []
        last_time = None
        time_to_break = False
        self.logger.debug(f'Started loading cache {cid}.')
        path_to_cache_dir = os.path.join(self.data_path, self._cache_name_template.format(cid))
        self.logger.debug(f'Path to cache {path_to_cache_dir}.')
        if not os.path.exists(path_to_cache_dir):
            self.logger.error(f'No cache with id={cid}')
            raise HTTPError(405, f'No cache with id={cid}')
        file_names = Path(path_to_cache_dir).glob('*.json')
        for file_name in file_names:
            if time_to_break:
                break
            self.logger.debug(f'Reading part: {file_name}')
            with FileReadBackwards(os.path.join(path_to_cache_dir, file_name)) as fr:
                for line in fr:
                    tmp = json.loads(line)
                    if last_time:
                        if last_time - tmp['_time'] > self.BIGGEST_INTERVAL:
                            time_to_break = True
                            break
                    else:
                        last_time = tmp['_time']
                    data.append(tmp)
        return data  # is not reversed intentionally. This way it is easier to build a timeline

    def _load_data_test(self, data_path):
        data = []
        last_time = None
        with FileReadBackwards(data_path) as fr:
            for line in fr:
                tmp = json.loads(line)
                if last_time:
                    if last_time - tmp['_time'] > self.BIGGEST_INTERVAL:
                        break
                else:
                    last_time = tmp['_time']
                data.append(tmp)
        return data  # is not reversed intentionally. This way it is easier to build a timeline
