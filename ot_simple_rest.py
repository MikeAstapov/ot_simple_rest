import json
import re

import tornado.ioloop
import tornado.web

import psycopg2




class MakeJob(tornado.web.RequestHandler):

    def post(self):
        response = make_job()
        return response

    def validate(self):
        # TODO
        return True

    def make_job(self):
        request = json.loads(self.request)
        original_spl = request["original_spl"]
        splitted_spl = original_spl.split('|')
        indexes = re.findall(r"index=(\S+)", splitted_spl[1])
        fields = re.findall(r"fields (\S+)+", splitted_spl[2])
        filters = list(filter(lambda x: 'index' not in x and 'AND' not in x, splitted_spl[1].split(' ')))
        tws = int(request['tws'])
        twf = int(request['twf'])
        calculation = splitted_spl[2:-1]
        make_job_statement = 'INSERT INTO splqueries (original_spl, indexes, fields, filters, tws, twf, calculation) VALUES (?,?,?,?,?,?,?);'
        if self.validate():
            self.cur.execute(make_job_statement, (original_spl, indexes, fields, filters, tws, twf, calculation))
            self.cur.commit()
            response = {"status": "Success", "job_id": 0} # TODO add real job_id
        else:
            response = {"status": "Failed", "error": "Validation failed"}
        retrun response

def main():
    db_conf = {
        "host": "c3000-blade2.corp.ot.ru",
        "database": "SuperVisor",
        "user": "postgres",
        "async": True
    }
    conn = psycopg2.connect(**db_conf)
    cur = conn.cursor()

    application = tornado.web.Application([
        (r'/makejob', MakeJob, {"cur": cur})
    ])
    application.listen(50000)
    tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
    main()
