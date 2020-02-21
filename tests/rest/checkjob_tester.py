import uuid
import re
from datetime import datetime

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
            'username': 'tester',
            'preview': False
        }

    def set_query(self, spl_query):
        self._current_spl = spl_query

    def tick_dispatcher(self):
        query_str = f"""INSERT INTO Ticks (applicationId) VALUES('test_app') 
        RETURNING extract(epoch from lastCheck) as lastCheck;"""
        self.db.cur.execute(query_str)
        self.db.conn.commit()

    def get_jobs_from_db(self, limit=None):
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

    def update_job_status(self, status, job_id):
        query_str = f"""UPDATE splqueries SET status='{status}' WHERE id={job_id};"""
        self.db.cur.execute(query_str)
        self.db.conn.commit()

    def add_job_with_cache(self):
        self.tick_dispatcher()
        job_id, _ = self.db.add_job(search=[self.original_spl, self._current_spl], subsearches=[], tws=0,
                                    twf=0, cache_ttl=60, username='tester', field_extraction=False, preview=False)
        self.db.add_to_cache(original_spl=self.original_spl, tws=0, twf=0, cache_id=job_id, expiring_date=1)
        return job_id

    def add_job_with_status(self, status):
        self.tick_dispatcher()
        job_id, _ = self.db.add_job(search=[self.original_spl, self.original_spl], subsearches=[], tws=0, twf=0,
                                    cache_ttl=60, username='tester', field_extraction=False, preview=False)
        self.update_job_status(status, job_id)

    @property
    def original_spl(self):
        original_spl = re.sub(r"\|\s*ot\s[^|]*\|", "", self._current_spl)
        original_spl = re.sub(r"\|\s*simple[^\"]*", "", original_spl)
        original_spl = original_spl.replace("oteval", "eval")
        original_spl = original_spl.strip()
        return original_spl

    def send_request(self):
        data = self.request_data
        data['original_spl'] = self._current_spl
        data['sid'] = str(uuid.uuid4())
        resp = requests.get(f'http://{self.config["host"]}:{self.config["port"]}/checkjob', params=data)
        resp.raise_for_status()
        return resp.json()

    def _cleanup(self):
        del_ticks_query = f"""DELETE FROM Ticks WHERE applicationId='test_app';"""
        del_spl_query = f"""DELETE FROM splqueries WHERE original_spl='{self.original_spl}';"""
        del_cache_query = f"""DELETE FROM cachesdl WHERE original_spl='{self.original_spl}';"""
        for query in [del_cache_query, del_ticks_query, del_spl_query]:
            self.db.cur.execute(query)
        self.db.conn.commit()

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
            self.db.add_job(search=[self.original_spl, self.original_spl], subsearches=[], tws=0, twf=0,
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
        try:
            job_id = self.add_job_with_cache()
            self.update_job_status('finished', job_id)
            resp = self.send_request()
        finally:
            self._cleanup()
        return resp == {'status': 'success', 'cid': job_id}

    def test__finished_nocache(self):
        try:
            self.add_job_with_status('finished')
            resp = self.send_request()
        finally:
            self._cleanup()
        return resp == {'status': 'nocache'}

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
