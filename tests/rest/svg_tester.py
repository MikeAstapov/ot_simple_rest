import os
import shutil

import requests


class SvgTester:
    """
    Test suite for /load/svg OT_REST endpoint.
    Testing for different job statuses:
    - load file
    - load duplicate file
    - delete file
    - delete non-existent file
    """

    def __init__(self, config, target_dir, test_file):
        self.cookies = None
        self.config = config
        self.target_dir = target_dir
        self.endpoint = '/api/load/svg'
        self.test_file = test_file

    def auth(self):
        data = {'username': 'admin', 'password': '12345678'}
        resp = requests.post(f'http://{self.config["host"]}:{self.config["port"]}/api/auth/login', json=data)
        resp.raise_for_status()
        self.cookies = resp.cookies

    def check_cookies(self):
        if not self.cookies:
            self.auth()

    def send_request(self, *, endpoint, method='GET', data=None, files=None):
        methods = {'GET': requests.get,
                   'POST': requests.post,
                   'PUT': requests.put,
                   'DELETE': requests.delete}
        self.check_cookies()
        req_method = methods.get(method)
        if not req_method:
            raise TypeError(f'unknown type of http method: {method}')

        resp = req_method(f'http://{self.config["host"]}:{self.config["port"]}{endpoint}',
                          cookies=self.cookies, data=data, files=files)
        resp.raise_for_status()
        return resp.json()

    def __cleanup_data(self, filename):
        path_for_remove = os.path.join(self.target_dir, filename)
        if os.path.exists(path_for_remove):
            os.remove(path_for_remove)

    def test__load_svg(self):
        try:
            file = self.test_file
            files = {'file': file}
            resp = self.send_request(endpoint=self.endpoint, method='POST', files=files)
        finally:
            self.__cleanup_data('file')
        return resp == {'status': 'ok', 'filename': 'file'}

    def test__load_duplicate(self):
        try:
            file = self.test_file
            files = {'file': file}
            resp = self.send_request(endpoint=self.endpoint, method='POST', files=files)

            file_1 = self.test_file
            files = {'file': file_1}
            resp = self.send_request(endpoint=self.endpoint, method='POST', files=files)
        finally:
            self.__cleanup_data('file')
            self.__cleanup_data('file_1')
        return resp == {'status': 'ok', 'filename': 'file_1'}

    def test__delete_svg(self):
        try:
            file = self.test_file
            files = {'file': file}
            resp = self.send_request(endpoint=self.endpoint, method='POST', files=files)

            data = {'filename': 'file'}
            resp = self.send_request(endpoint=self.endpoint, method='DELETE', data=data)
        finally:
            self.__cleanup_data('file')
        return resp == {'status': 'ok'}

    def test__delete_nonexistent(self):
        data = {'filename': 'file'}
        resp = self.send_request(endpoint=self.endpoint, method='DELETE', data=data)
        return resp == {'status': 'failed', 'error': 'file not found'}
