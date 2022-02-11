from .base_loader import BaseLoader
from pathlib import Path
import os
import pandas as pd
from tornado.web import HTTPError


class InterestingFieldsLoader(BaseLoader):

    """
    main purpose to load data from cid and return the data as a dataframe
    """

    def __init__(self, mem_conf, static_conf):
        super().__init__(mem_conf, static_conf)

    def load_data(self, cid):
        """
        Load data by cid

        :param cid:         OT_Dispatcher's job cid
        :return:            pandas dataframe
        """
        data = None
        self.logger.debug(f'Started loading cache {cid}.')
        path_to_cache_dir = os.path.join(self.data_path, self._cache_name_template.format(cid))
        self.logger.debug(f'Path to cache {path_to_cache_dir}.')
        if not os.path.exists(path_to_cache_dir):
            self.logger.error(f'No cache with id={cid}')
            raise HTTPError(405, f'No cache with id={cid}')
        file_names = Path(path_to_cache_dir).glob('*.json')
        for file_name in file_names:
            self.logger.debug(f'Reading part: {file_name}')
            df = pd.read_json(os.path.join(path_to_cache_dir, file_name), lines=True, convert_dates=False)
            if not data:
                data = df
            else:
                data.append(df, ignore_index=True)
        return data
