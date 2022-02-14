import logging
from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseLoader(ABC):

    """
    Interface for loaders, that load data using only cid
    """

    def __init__(self, mem_conf: Dict, static_conf: Dict):
        self.mem_conf = mem_conf
        self.static_conf = static_conf
        self.data_path = self.mem_conf['path']
        self.logger = logging.getLogger('osr')
        self._cache_name_template = 'search_{}.cache/data'

    @abstractmethod
    def load_data(self, cid: str) -> Any: raise NotImplementedError

