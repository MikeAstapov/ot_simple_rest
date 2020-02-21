#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ot_simple_rest.py

import logging
import os
from configparser import ConfigParser

import tornado.ioloop
import tornado.web

from handlers.jobs.makejob import MakeJob
from handlers.jobs.loadjob import LoadJob
from handlers.jobs.checkjob import CheckJob
from handlers.jobs.getdata import GetResult
from handlers.jobs.saveotrest import SaveOtRest
from handlers.service.makerolemodel import MakeRoleModel
from handlers.service.makedatamodels import MakeDataModels
from handlers.service.pingpong import PingPong

from jobs_manager.manager import JobsManager

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Anton Khromov"]
__license__ = ""
__version__ = "0.14.4"
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

    # # # # # # #  Configuration section  # # # # # # #

    basedir = os.path.dirname(os.path.abspath(__file__))

    config = ConfigParser()
    config.read(os.path.join(basedir, 'ot_simple_rest.conf'))

    db_conf = dict(config['db_conf'])
    mem_conf = dict(config['mem_conf'])
    disp_conf = dict(config['dispatcher'])
    resolver_conf = dict(config['resolver'])
    static_conf = dict(config['static'])
    user_conf = dict(config['user'])  # TODO if you think if "dict" is not needed, remove it from other configs.

    # # # # # # # # # # # # # # # # # # # # # # # # # #

    logger = set_logger(config['general'].get('level', 'INFO'), './logs/otsimplerest.log', 'osr')
    logger.info('DB configuration: %s' % db_conf)
    logger.info('MEM configuration: %s' % mem_conf)

    # Create jobs manager instance with configs needed to jobs work

    manager = JobsManager(db_conf=db_conf, mem_conf=mem_conf, disp_conf=disp_conf,
                          resolver_conf=resolver_conf, user_conf=user_conf)
    manager.start()

    # Set TORNADO application with custom handlers.
    application = tornado.web.Application([
        (r'/ping', PingPong),
        # TODO You need to save old endpoint for backward compatibility.
        (r'/checkjob', CheckJob, {"manager": manager}),
        (r'/getresult', GetResult, {"mem_conf": mem_conf, "static_conf": static_conf}),
        (r'/makejob', MakeJob, {"manager": manager}),
        (r'/loadjob', LoadJob, {"manager": manager}),
        (r'/otrest', SaveOtRest, {"db_conf": db_conf, "mem_conf": mem_conf}),
        (r'/makerolemodel', MakeRoleModel, {"db_conf": db_conf}),
        (r'/makedatamodels', MakeDataModels, {"db_conf": db_conf})
    ])

    logger.info('Starting server')

    # Start application.
    application.listen(50000)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
