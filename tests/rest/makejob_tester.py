import re
from datetime import datetime
import uuid
import time

import requests


class MakejobTester:
    """
    Test suite for /makejob OT_REST endpoint.
    Testing for different job statuses:
    - job not created
    - status is 'new'
    - status is 'running'
    - status is 'finished'
    - status is 'failed'
    - status is 'external'
    - status is 'canceled'
    """

    def __init__(self, db, config):
        self.db = db
        self.config = config

        self._current_spl = None

        self.request_data = {
            'sid': None,
            'original_spl': None,
            'tws': 0,
            'twf': 0,
            'cache_ttl': 60,
            'field_extraction': False,
            'username': 'admin',
            'preview': False
        }

    def set_query(self, spl_query):
        self._current_spl = spl_query

    def update_job_status(self, status, job_id):
        query_str = f"""UPDATE splqueries SET status='{status}' WHERE id={job_id};"""
        self.db.cur.execute(query_str, (status, job_id))
        self.db.conn.commit()

    @property
    def original_spl(self):
        original_spl = re.sub(r"\|\s*ot\s[^|]*\|", "", self._current_spl)
        original_spl = re.sub(r"\|\s*simple[^\"]*", "", original_spl)
        original_spl = original_spl.replace("oteval", "eval")
        original_spl = original_spl.strip()
        return original_spl

    def get_jobs_from_db(self, limit=None):
        """
        Return num=limit entries from DB

        :param limit:       number of entries
        :return:
        """
        query_str = f"""SELECT id, creating_date, status FROM splqueries 
        WHERE original_spl='{self.original_spl}' ORDER BY creating_date DESC;"""
        if limit:
            query_str = query_str.replace(';', f' LIMIT {limit};')
        self.db.cur.execute(query_str)

        if limit == 1:
            result = self.db.cur.fetchone()
            if not result:
                return None
            job_id, creating_date, status = result
            return {'id': job_id, 'date': datetime.strftime(creating_date, '%Y-%m-%d %H:%M:%S'), 'status': status}
        else:
            return self.db.cur.fetchall()

    def _cleanup(self):
        del_spl_query = f"""DELETE FROM splqueries WHERE original_spl='{self.original_spl}';"""
        del_cache_query = f"""DELETE FROM cachesdl WHERE original_spl='{self.original_spl}';"""
        del_splunksids_query = """DELETE FROM splunksids;"""
        for query in [del_spl_query, del_cache_query, del_splunksids_query]:
            self.db.cur.execute(query)
        self.db.conn.commit()

    def send_request(self):
        data = self.request_data
        data['original_spl'] = self.original_spl
        data['sid'] = str(uuid.uuid4())
        resp = requests.post(f'http://{self.config["host"]}:{self.config["port"]}/makejob', data=data)
        resp.raise_for_status()
        return resp.json()

    def send_and_validate(self, need_new_job):
        jobs_before = len(self.get_jobs_from_db())
        resp = self.send_request()

        if not resp or resp['status'] != 'success':
            raise ConnectionError('Request failed')
        time.sleep(0.5)  # Wait for queue processing
        jobs_after = len(self.get_jobs_from_db())

        diff = 1 if need_new_job else 0
        if jobs_after - jobs_before != diff:
            raise ValueError(f'Number of jobs before {jobs_before} and after {jobs_after} not valid')

        job_data = self.get_jobs_from_db(limit=1)
        if not job_data:
            raise ValueError('Job does not exist')

        return job_data

    def add_job_with_status(self, status):
        job_id, _ = self.db.add_job(search=[self.original_spl, self.original_spl], subsearches=[], tws=0, twf=0,
                                    cache_ttl=60, username='tester', field_extraction=False, preview=False)
        self.update_job_status(status, job_id)

    def add_job_with_cache(self, need_expired):
        job_id, _ = self.db.add_job(search=[self.original_spl, self.original_spl], subsearches=[], tws=0, twf=0,
                                    cache_ttl=60, username='tester', field_extraction=False, preview=False)
        self.update_job_status('finished', job_id)
        self.db.add_to_cache(original_spl=self.original_spl, tws=0, twf=0,
                             cache_id=100500, expiring_date=1)
        if need_expired:
            time.sleep(1)

    def test__no_job(self):
        try:
            job_data = self.send_and_validate(need_new_job=True)
        finally:
            self._cleanup()
        return job_data['status'] == 'new'

    def test__new(self):
        try:
            self.add_job_with_status('new')
            job_data = self.send_and_validate(need_new_job=True)
        finally:
            self._cleanup()
        return job_data['status'] == 'new'

    def test__running(self):
        try:
            self.add_job_with_status('running')
            job_data = self.send_and_validate(need_new_job=False)
        finally:
            self._cleanup()
        return job_data['status'] == 'running'

    def test__finished(self):
        try:
            self.add_job_with_cache(need_expired=False)
            job_data = self.send_and_validate(need_new_job=False)
        finally:
            self._cleanup()
        return job_data['status'] == 'finished'

    def test__finished_expired(self):
        try:
            self.add_job_with_cache(need_expired=True)
            job_data = self.send_and_validate(need_new_job=True)
        finally:
            self._cleanup()
        return job_data['status'] == 'new'

    def test__failed(self):
        try:
            self.add_job_with_status('failed')
            job_data = self.send_and_validate(need_new_job=True)
        finally:
            self._cleanup()
        return job_data['status'] == 'new'

    def test__canceled(self):
        try:
            self.add_job_with_status('canceled')
            job_data = self.send_and_validate(need_new_job=True)
        finally:
            self._cleanup()
        return job_data['status'] == 'new'

    def test__external(self):
        try:
            self.add_job_with_status('external')
            job_data = self.send_and_validate(need_new_job=True)
        finally:
            self._cleanup()
        return job_data['status'] == 'new'
