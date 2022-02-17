import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator
from pathlib import Path
from tornado.web import HTTPError


class BaseLoader(ABC):

    """
    Base class for loaders, that loads data using only cid
    """

    def __init__(self, mem_conf: Dict, static_conf: Dict):
        self.mem_conf = mem_conf
        self.static_conf = static_conf
        self.data_path = self.mem_conf['path']
        self.logger = logging.getLogger('osr')
        self._cache_name_template = 'search_{}.cache/data'

    def _get_path_to_cache_dir(self, cid) -> str:
        self.logger.debug(f'Started loading cache {cid}.')
        return os.path.join(self.data_path, self._cache_name_template.format(cid))

    def _get_cache_file_names(self, path_to_cache_dir, cid: str) -> Generator:
        self.logger.debug(f'Path to cache {path_to_cache_dir}.')
        if not os.path.exists(path_to_cache_dir):
            self.logger.error(f'No cache with id={cid}')
            raise HTTPError(405, f'No cache with id={cid}')
        return Path(path_to_cache_dir).glob('*.json')

    @abstractmethod
    def load_data(self, cid: str) -> Any:
        """Implement data loading"""
        raise NotImplementedError

