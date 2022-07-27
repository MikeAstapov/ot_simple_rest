import os
import uuid
import shutil
import re
from datetime import datetime
from utils.hashes import hash512

import requests


class CheckjobTester:
    """
    Test suite for /checkjob OT_REST endpoint.
    Testing for different job statuses:
    - job not created
    - status is 'new'
    - status is 'running'
    - status is 'finished'
    - status is 'failed'
    - status is 'canceled'
    """

    def __init__(self, db, config, path):
        self.db = db
        self.config = config
        self.cache_dir = path

        self._current_otl = None

        self.request_data = {
            'sid': None,
            'original_otl': None,
            'tws': 0,
            'twf': 0,
            'cache_ttl': 60,
            'field_extraction': False,
            'username': 'tester',
            'preview': False
        }

    def set_query(self, otl_query):
        self._current_otl = otl_query

    def tick_dispatcher(self):
        query_str = f"""INSERT INTO Ticks (applicationId) VALUES('test_app') 
        RETURNING extract(epoch from lastCheck) as lastCheck;"""
        self.db.execute_query(query_str, with_commit=True)

    def get_jobs_from_db(self, limit=None):
        query_str = f"""SELECT id, creating_date, status FROM OTLQueries 
        WHERE original_otl='{self.original_otl}' ORDER BY creating_date DESC;"""
        if limit:
            query_str = query_str.replace(';', f' LIMIT {limit};')

        if limit == 1:
            result = self.db.execute_query(query_str)
            if not result:
                return None
            job_id, creating_date, status = result
            return {'id': job_id, 'date': datetime.strftime(creating_date, '%Y-%m-%d %H:%M:%S'), 'status': status}
        else:
            return self.db.cur.execute(query_str, fetchall=True)

    def update_job_status(self, status, job_id):
        query_str = f"""UPDATE OTLQueries SET status='{status}' WHERE id={job_id};"""
        self.db.execute_query(query_str, with_commit=True, with_fetch=False)

    def add_job_with_cache(self):
        self.tick_dispatcher()
        job_id, _ = self.db.add_job(search=[self.original_otl, self._current_otl], subsearches=[], tws=0,
                                    twf=0, cache_ttl=60, username='tester', field_extraction=False, preview=False)
        self.db.add_to_cache(original_otl=self.original_otl, tws=0, twf=0, cache_id=job_id, expiring_date=1)
        # import time
        # time.sleep(100)
        return job_id

    def add_job_with_status(self, status):
        self.tick_dispatcher()
        job_id, _ = self.db.add_job(search=[self.original_otl, self.original_otl], subsearches=[], tws=0, twf=0,
                                    cache_ttl=60, username='tester', field_extraction=False, preview=False)
        self.update_job_status(status, job_id)

    @property
    def original_otl(self):
        original_otl = re.sub(r"\|\s*ot\s[^|]*\|", "", self._current_otl)
        original_otl = re.sub(r"\|\s*simple[^\"]*", "", original_otl)
        original_otl = original_otl.replace("oteval", "eval ")
        original_otl = original_otl.strip()
        return original_otl

    def send_request(self):
        data = self.request_data
        data['original_otl'] = self._current_otl
        data['sid'] = str(uuid.uuid4())
        resp = requests.get(f'http://{self.config["host"]}:{self.config["port"]}/api/checkjob', params=data)
        resp.raise_for_status()
        return resp.json()

    def prepare_limited_data(self, cid):
        full_path = os.path.join(self.cache_dir, f'search_{cid}.cache/data/')
        try:
            os.makedirs(full_path, exist_ok=True)
        except PermissionError as err:
            print(err)
        file = open(os.path.join(full_path, 'data_part_000.json'), 'a')
        file.write("{'t': '0'}\n" * 1000000)
        file.close()
        open(os.path.join(full_path, '_SCHEMA'), 'a').close()

    def prepare_data(self, cid):
        full_path = os.path.join(self.cache_dir, f'search_{cid}.cache/data/')
        try:
            os.makedirs(full_path, exist_ok=True)
        except PermissionError as err:
            print(err)
        open(os.path.join(full_path, 'data_part_000.json'), 'a').close()
        open(os.path.join(full_path, '_SCHEMA'), 'a').close()

    def _cleanup(self):
        del_ticks_query = f"""DELETE FROM Ticks WHERE applicationId='test_app';"""
        del_otl_query = f"""DELETE FROM OTLQueries WHERE original_otl='{self.original_otl}';"""
        del_cache_query = f"""DELETE FROM cachesdl WHERE original_otl='{hash512(self.original_otl)}';"""
        for query in [del_cache_query, del_ticks_query, del_otl_query]:
            self.db.execute_query(query, with_commit=True, with_fetch=False)

    def _cleanup_data(self, cid):
        full_path = os.path.join(self.cache_dir, f'search_{cid}.cache')
        if os.path.exists(full_path):
            shutil.rmtree(full_path)

    def test__no_job(self):
        try:
            self.tick_dispatcher()
            resp = self.send_request()
        finally:
            self._cleanup()
        return resp == {'status': 'notfound', 'error': 'Job is not found'}

    def test__new(self):
        try:
            self.tick_dispatcher()
            self.db.add_job(search=[self.original_otl, self.original_otl], subsearches=[], tws=0, twf=0,
                            cache_ttl=60, username='tester', field_extraction=False, preview=False)
            resp = self.send_request()
        finally:
            self._cleanup()
        return resp == {'status': 'new'}

    def test__running(self):
        try:
            self.add_job_with_status('running')
            resp = self.send_request()
        finally:
            self._cleanup()
        return resp == {'status': 'running'}

    def test__finished(self):
        job_id = 0
        try:
            job_id = self.add_job_with_cache()
            self.update_job_status('finished', job_id)
            self.prepare_data(job_id)
            resp = self.send_request()
        finally:
            self._cleanup()
            self._cleanup_data(job_id)
        return resp == {'status': 'success', 'cid': job_id, 'lines': 0}

    def test__finished_nocache(self):
        try:
            self.add_job_with_status('finished')
            resp = self.send_request()
        finally:
            self._cleanup()
        return resp == {'status': 'nocache', 'error': 'No cache for this job'}

    def test__failed(self):
        try:
            self.add_job_with_status('failed')
            resp = self.send_request()
        finally:
            self._cleanup()
        return resp['status'] == 'failed'

    def test__canceled(self):
        try:
            self.add_job_with_status('canceled')
            resp = self.send_request()
        finally:
            self._cleanup()
        return resp['status'] == 'canceled'

    def test__limited_data(self):
        job_id = 0
        try:
            job_id = self.add_job_with_cache()
            self.update_job_status('finished', job_id)
            self.prepare_limited_data(job_id)
            resp = self.send_request()
        finally:
            self._cleanup()
            self._cleanup_data(job_id)
        return {'code': 2, 'value': 1000000} in resp.get('notifications', [])
