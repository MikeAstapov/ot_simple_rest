import logging

import tornado.web
import psycopg2
from pyignite import Client


class LoadJob(tornado.web.RequestHandler):

    def initialize(self, db_conf, ignite_conf):
        self.db_conf = db_conf
        self.ignite_conf = ignite_conf
        self.logger = logging.getLogger('osr')
        self.logger.debug('Initialized')

    def get(self):
        response = self.load_job()
        self.write(response)

    def load_job(self):
        request = self.request.arguments
        self.logger.debug(request)
        original_spl = request["original_spl"][0].decode()
        tws = int(float(request['tws'][0]))
        twf = int(float(request['twf'][0]))

        conn = psycopg2.connect(**self.db_conf)
        cur = conn.cursor()

        check_job_status = 'SELECT splqueries.id, splqueries.status, cachesdl.expiring_date FROM splqueries ' \
                           'LEFT JOIN cachesdl ON splqueries.id = cachesdl.id WHERE splqueries.original_spl=%s AND ' \
                           'splqueries.tws=%s AND splqueries.twf=%s ORDER BY splqueries.id DESC LIMIT 1 '

        self.logger.info(check_job_status % (original_spl, tws, twf))
        cur.execute(check_job_status, (original_spl, tws, twf))
        fetch = cur.fetchone()
        self.logger.info(fetch)
        if fetch:
            cid, status, expiring_date = fetch
            if status == 'finished' and expiring_date:
                events = self.ignite_load(cid)
                self.logger.info('Cache is %s loaded from ignite.' % cid)
                response = {'status': 'success', 'events': events}
            elif status == 'finished' and not expiring_date:
                response = {'status': 'nocache'}
            elif status == 'running':
                response = {'status': 'running'}
            elif status == 'new':
                response = {'status': 'new'}
            elif status == 'fail':
                response = {'status': 'fail', 'error': 'Job is failed'}
            else:
                self.logger.warning('Unknown status of job: %s' % status)
                response = {'status': 'fail', 'error': 'Unknown status: %s' % status}
        else:
            response = {'status': 'fail', 'error': 'Job is not found'}

        self.logger.debug(response)
        return response

    def ignite_load(self, cid):
        events = []
        client = Client()
        client.connect(self.ignite_conf['nodes'])
        cache = client.get_cache('SQL_PUBLIC_CACHE_%s' % cid)
        results = cache.scan()
        for k, v in results:
            fields = v.schema.keys()
            event = {}
            for field in fields:
                event[field.lower()] = getattr(v, field)
            events.append(event)
            if len(events) == 100000:
                break
        client.close()
        return events

