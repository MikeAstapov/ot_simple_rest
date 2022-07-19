from .base_loader import BaseLoader
import os
import pandas as pd
from pandas import DataFrame as PandasDataFrame
from typing import Dict, Optional


class InterestingFieldsLoader(BaseLoader):

    """
    Main purpose to load data from cid and return the data as a dataframe.
    """

    def __init__(self, mem_conf: Dict, static_conf: Dict):
        super().__init__(mem_conf, static_conf)

    def load_data(self, cid: str, from_time: Optional[int] = None, to_time: Optional[int] = None) -> PandasDataFrame:
        """
        Load data by cid

        :param cid:         OT_Dispatcher's job cid
        :param from_time:         loads data from that moment
        :param to_time:         loads data till that moment
        :return:            pandas dataframe
        """
        data = None
        path_to_cache_dir = self._get_path_to_cache_dir(cid)
        file_names = self._get_cache_file_names(path_to_cache_dir, cid)
        for file_name in file_names:
            self.logger.debug(f'Reading part: {file_name}')
            df = pd.read_json(file_name, lines=True, convert_dates=False)
            if not data:
                data = df
            else:
                data.append(df, ignore_index=True)
        if from_time:
            data = data[data['_time'] >= from_time]
        if to_time:
            data = data[data['_time'] <= to_time]
        return data
