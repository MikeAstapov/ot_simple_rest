import logging
from abc import ABC, abstractmethod


class BaseLoader(ABC):

    def __init__(self, mem_conf, static_conf):
        self.mem_conf = mem_conf
        self.static_conf = static_conf
        self.data_path = self.mem_conf['path']
        self.logger = logging.getLogger('osr')
        self._cache_name_template = 'search_{}.cache/data'

    @abstractmethod
    def load_data(self, cid): raise NotImplementedError

