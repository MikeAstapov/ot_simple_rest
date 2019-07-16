#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ot_simple_rest.py

import logging

import tornado.ioloop
import tornado.web

from handlers.loadjob import LoadJob
from handlers.makejob import MakeJob
from handlers.makerolemodel import MakeRoleModel
from handlers.saveotrest import SaveOtRest
from handlers.pingpong import PingPong
from handlers.blockingHandler import BlockingHandler

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = []
__license__ = ""
__version__ = "0.3.0"
__maintainer__ = "Andrey Starchenkov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Development"


def set_logger(loglevel, logfile, logger_name):

    levels = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG
        }

    logging.basicConfig(
        filename=logfile,
        level=levels[loglevel],
        format="%(asctime)s %(levelname)-s PID=%(process)d %(module)s:%(lineno)d \
func=%(funcName)s - %(message)s")

    logger = logging.getLogger(logger_name)
    return logger


def main():
    """
    Main function with config and starter code. It starts TORNADO server with custom handlers which register, check and
    load Job's results from ramcache to OT.Simple Splunk app.
    :return:
    """

    logger = set_logger('DEBUG', './otsimplerest.log', 'osr')

    # # # # # # #  Configuration section  # # # # # # #

    # TODO replace this section with config files

    # Configuration of Postgres DB.
    db_conf = {
        "host": "am.local",
        "database": "dispatcher",
        "user": "dispatcher",
        "password": "P@$$w0rd"
        # "async": True
    }

    # Configuration of ramcache.
    mem_conf = {
        "path": "/mnt/g1/caches/"
    }

    # # # # # # # # # # # # # # # # # # # # # # # # # #

    logger.info('DB configuration: %s' % db_conf)
    logger.info('MEM configuration: %s' % mem_conf)

    # Set TORNADO application with custom handlers.
    application = tornado.web.Application([
        (r'/ping', PingPong),
        (r'/makejob', MakeJob, {"db_conf": db_conf}),
        (r'/loadjob', LoadJob, {"db_conf": db_conf, "mem_conf": mem_conf}),
        (r'/otrest', SaveOtRest, {"db_conf": db_conf, "mem_conf": mem_conf}),
        (r'/makerolemodel', MakeRoleModel, {"db_conf": db_conf}),
        (r'/blocking', BlockingHandler)
    ])

    logger.info('Starting server')

    # Start application.
    application.listen(50000)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
