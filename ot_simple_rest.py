import json
import re

import tornado.ioloop
import tornado.web

import psycopg2


class MakeJob(tornado.web.RequestHandler):

    def initialize(self, cur, conn):
        self.cur = cur
        self.conn = conn

    def post(self):
        # TODO check for cache
        response = self.make_job()
        self.write(json.dumps(response))



    @staticmethod
    def validate():
        # TODO
        return True

    def check_cache(self, cache_ttl, original_spl):
        cache_id = creating_date = None
        if cache_ttl:
            check_cache_statement = 'SELECT id, creating_date FROM cachesdl WHERE expiring_date >= CURRENT_TIMESTAMP AND original_spl=%s'
            self.cur.execute(check_cache_statement, (original_spl,))
            fetch = self.cur.fetchone()
            print(fetch)
            if fetch:
                cache_id, creating_date = fetch[0]
        return cache_id, creating_date

    def make_job(self):
        print(self.request.body_arguments)
        request = self.request.body_arguments
        original_spl = request["original_spl"][0].decode()
        cache_ttl = request['cache_ttl'][0]
        cache_id, creating_date = self.check_cache(cache_ttl, original_spl)
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
            make_job_statement = 'INSERT INTO splqueries \
            (original_spl, indexes, fields, filters, tws, twf, calculation, cache_ttl) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id, extract(epoch from creating_date);'
            if self.validate():
                self.cur.execute(make_job_statement, (
                    original_spl, indexes, fields, filters, tws, twf, calculation, cache_ttl
                ))
                job_id, _time = self.cur.fetchone()
                self.conn.commit()
                response = {"_time": _time, "status": "Success", "job_id": job_id}
            else:
                response = {"status": "Failed", "error": "Validation failed"}
        else:
            response = {"_time": creating_date, "status": "Success", "job_id": cache_id}

        print(response)
        return response


def main():
    db_conf = {
        "host": "c3000-blade2.corp.ot.ru",
        "database": "SuperVisor",
        "user": "postgres",
        # "async": True
    }
    conn = psycopg2.connect(**db_conf)
    cur = conn.cursor()

    application = tornado.web.Application([
        (r'/makejob', MakeJob, {"cur": cur, "conn": conn})
    ])
    application.listen(50000)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
