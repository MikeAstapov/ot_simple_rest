from .base_loader import BaseLoader
import os
import pandas as pd


class InterestingFieldsLoader(BaseLoader):

    """
    main purpose to load data from cid and return the data as a dataframe
    """

    def __init__(self, mem_conf, static_conf):
        super().__init__(mem_conf, static_conf)

    def _load_data_test(self, data_path):
        return pd.read_json(data_path, lines=True, convert_dates=False)

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
        file_names = [file_name for file_name in os.listdir(path_to_cache_dir) if file_name[-5:] == '.json']
        for file_name in file_names:
            self.logger.debug(f'Reading part: {file_name}')
            df = pd.read_json(os.path.join(path_to_cache_dir, file_name), lines=True, convert_dates=False)
            if not data:
                data = df
            else:
                data.append(df, ignore_index=True)
        return data
