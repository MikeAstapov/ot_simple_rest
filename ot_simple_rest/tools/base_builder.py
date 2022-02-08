import logging
import os
from abc import ABC
import pandas as pd


class BaseBuilder(ABC):

    def __init__(self, mem_conf, static_conf):
        self.mem_conf = mem_conf
        self.static_conf = static_conf
        self.data_path = self.mem_conf['path']
        self.logger = logging.getLogger('osr')
        self._cache_name_template = 'search_{}.cache/data'

    @staticmethod
    def _load_json_lines_test(data_path):
        return pd.read_json(data_path, lines=True)

    def _load_json_lines(self, cid):
        """
        Load data by cid

        :param cid:         OT_Dispatcher's job cid
        :return:            pandas dataframe
        """
        data = None
        self.logger.debug(f'Started loading cache {cid}.')
        path_to_cache_dir = os.path.join(self.data_path, self._cache_name_template.format(cid))
        self.logger.debug(f'Path to cache {path_to_cache_dir}.')
        file_names = [file_name for file_name in os.listdir(path_to_cache_dir) if file_name[-5:] == '.json']
        for file_name in file_names:
            self.logger.debug(f'Reading part: {file_name}')
            df = pd.read_json(os.path.join(path_to_cache_dir, file_name), lines=True)
            if not data:
                data = df
            else:
                data.append(df, ignore_index=True)
        return data
