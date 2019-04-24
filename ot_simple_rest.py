import json
import re
from time import sleep

import tornado.ioloop
import tornado.web

import psycopg2


class MakeJob(tornado.web.RequestHandler):

    def initialize(self, db_conf):
        self.db_conf = db_conf

    def post(self):
        response = self.make_job()
        self.write(json.dumps(response))

    @staticmethod
    def validate():
        # TODO
        return True

    def check_cache(self, cache_ttl, original_spl, cur):
        cache_id = creating_date = None
        if cache_ttl:
            check_cache_statement = 'SELECT id, extract(epoch from creating_date) FROM cachesdl WHERE expiring_date >= \
            CURRENT_TIMESTAMP AND original_spl=%s'
            cur.execute(check_cache_statement, (original_spl,))
            fetch = cur.fetchone()
            print(fetch)
            if fetch:
                cache_id, creating_date = fetch
        return cache_id, creating_date

    def check_running(self, original_spl, cur):
        check_running_statement = "SELECT id, extract(epoch from creating_date) FROM splqueries WHERE status = 'running' AND original_spl=%s"
        cur.execute(check_running_statement, (original_spl,))
        fetch = cur.fetchone()
        print(fetch)

        if fetch:
            job_id, creating_date = fetch
        else:
            job_id = creating_date = None

        return job_id, creating_date

    def make_job(self):
        print(self.request.body_arguments)
        request = self.request.body_arguments
        original_spl = request["original_spl"][0].decode()
        cache_ttl = int(request['cache_ttl'][0])


        conn = psycopg2.connect(**self.db_conf)
        cur = conn.cursor()
        
        cache_id, creating_date = self.check_cache(cache_ttl, original_spl, cur)

        if cache_id is None:

            splitted_spl = original_spl.split('|')
            indexes = re.findall(r"index=(\S+)", splitted_spl[2])
            fields = re.findall(r"\\| ?fields (\S+)+", original_spl)
            filters = list(filter(
                lambda x: 'index' not in x and 'AND' not in x and x, splitted_spl[2].strip().split(' ')[1:]
            ))
            tws = int(float(request['tws'][0]))
            twf = int(float(request['twf'][0]))
            calculation = splitted_spl[4:-1]

            if self.validate():

                job_id, creating_date = self.check_running(original_spl, cur)

                if job_id is None:

                    make_job_statement = 'INSERT INTO splqueries \
                    (original_spl, indexes, fields, filters, tws, twf, calculation, cache_ttl) \
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id, extract(epoch from creating_date);'
                    cur.execute(make_job_statement, (
                        original_spl, indexes, fields, filters, tws, twf, calculation, cache_ttl
                    ))
                    job_id, creating_date = cur.fetchone()
                    conn.commit()

                response = {"_time": creating_date, "status": "Success", "job_id": job_id}
            else:
                response = {"status": "Failed", "error": "Validation failed"}
        else:
            response = {"_time": creating_date, "status": "Success", "job_id": cache_id}

        print(response)
        return response


class LoadJob(tornado.web.RequestHandler):

    def initialize(self, db_conf):
        self.db_conf = db_conf

    def get(self):
        response = self.load_job()
        self.write(json.dumps(response))

    def load_job(self):
        request = self.request.arguments
        print(request)
        original_spl = request["original_spl"][0].decode()
        tws = int(float(request['tws'][0]))
        twf = int(float(request['twf'][0]))

        conn = psycopg2.connect(**self.db_conf)
        cur = conn.cursor()

        check_cache_statement = 'SELECT id, extract(epoch from creating_date) FROM cachesdl WHERE expiring_date >= \
        CURRENT_TIMESTAMP AND original_spl=%s'

        fetch = False
        while not fetch:
            cur.execute(check_cache_statement, (original_spl,))
            fetch = cur.fetchone()
            sleep(1)

        print(fetch)
        cache_id, creating_date = fetch
        response = {"_time": creating_date, "status": "Success", "cache_id": cache_id}
        return response


def main():

    db_conf = {
        "host": "c3000-blade2.corp.ot.ru",
        "database": "SuperVisor",
        "user": "postgres",
        # "async": True
    }

    application = tornado.web.Application([
        (r'/makejob', MakeJob, {"db_conf": db_conf}),
        (r'/loadjob', LoadJob, {"db_conf": db_conf})
    ])
    application.listen(50000)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
