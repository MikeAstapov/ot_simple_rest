from time import sleep

import tornado.web
import psycopg2


class LoadJob(tornado.web.RequestHandler):

    def initialize(self, db_conf):
        self.db_conf = db_conf

    def get(self):
        response = self.load_job()
        self.write(response)

    def load_job(self):
        request = self.request.arguments
        print('load_job', request)
        original_spl = request["original_spl"][0].decode()
        tws = int(float(request['tws'][0]))
        twf = int(float(request['twf'][0]))

        conn = psycopg2.connect(**self.db_conf)
        cur = conn.cursor()

        check_cache_statement = 'SELECT id, extract(epoch from creating_date) FROM cachesdl WHERE expiring_date >= \
        CURRENT_TIMESTAMP AND original_spl=%s AND tws=%s AND twf=%s;'

        cur.execute(check_cache_statement, (original_spl, tws, twf))
        fetch = cur.fetchone()
        print(fetch)
        if fetch:
            cache_id, creating_date = fetch
            response = {"_time": creating_date, "status": "Success", "cache_id": cache_id, 'status': 'Finished'}
        else:
            response = {"status": "Running"}
        return response
