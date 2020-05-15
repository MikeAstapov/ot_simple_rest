import os
import shutil

import requests


class GetresultTester:
    """
    Test suite for /api/getresult OT_REST endpoint.
    Test case:
    - returns list of data links
    """

    def __init__(self, api_conf, mem_conf, static_conf):
        self.config = api_conf
        self.cache_path = mem_conf['path']
        self.cache_template = static_conf['base_url']
        self.test_data_path = 'search_100500.cache/data'

    def prepare_data(self):
        full_path = os.path.join(self.cache_path, self.test_data_path)
        try:
            os.makedirs(full_path, exist_ok=True)
        except PermissionError as err:
            print(err)
        open(os.path.join(full_path, 'data_part_000.json'), 'a').close()
        open(os.path.join(full_path, 'data_part_001.json'), 'a').close()
        open(os.path.join(full_path, '_SCHEMA'), 'a').close()

    def _cleanup(self):
        path_for_remove = os.path.join(self.cache_path, os.path.dirname(self.test_data_path))
        if os.path.exists(path_for_remove):
            shutil.rmtree(path_for_remove)
        
    def send_request(self):
        resp = requests.get(f'http://{self.config["host"]}:{self.config["port"]}/api/getresult',
                            params={'cid': 100500})
        resp.raise_for_status()
        return resp.json()

    def test__getresult(self):
        data_urls = {self.cache_template.format('search_100500.cache/data/data_part_000.json'),
                     self.cache_template.format('search_100500.cache/data/data_part_001.json'),
                     self.cache_template.format('search_100500.cache/data/_SCHEMA')}
        try:
            self.prepare_data()
            job_data = self.send_request()
        finally:
            self._cleanup()
        return set(job_data.get('data_urls')) == data_urls
