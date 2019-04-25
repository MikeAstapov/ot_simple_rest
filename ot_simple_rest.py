import tornado.ioloop
import tornado.web

from handlers.loadjob import LoadJob
from handlers.makejob import MakeJob
from handlers.makerolemodel import MakeRoleModel


def main():

    db_conf = {
        "host": "c3000-blade2.corp.ot.ru",
        "database": "SuperVisor",
        "user": "postgres",
        # "async": True
    }

    application = tornado.web.Application([
        (r'/makejob', MakeJob, {"db_conf": db_conf}),
        (r'/loadjob', LoadJob, {"db_conf": db_conf}),
        (r'/makerolemodel', MakeRoleModel, {"db_conf": db_conf})
    ])

    application.listen(50000)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
