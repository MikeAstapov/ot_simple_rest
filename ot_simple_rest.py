#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ot_simple_rest.py

import logging
import os
from configparser import ConfigParser

import tornado.ioloop
import tornado.web

from psycopg2.pool import ThreadedConnectionPool

from handlers.eva.auth import AuthLoginHandler
from handlers.eva.dashs import DashboardHandler, DashboardsHandler
from handlers.eva.quizs import QuizHandler, QuizsHandler
from handlers.eva.role_model import UserHandler, UsersHandler, RoleHandler, RolesHandler, \
    PermissionsHandler, PermissionHandler, GroupsHandler, GroupHandler, UserPermissionsHandler, \
    IndexesHandler, IndexHandler, UserGroupsHandler, UserDashboardsHandler, GroupDashboardsHandler

# from handlers.eva.auth import AuthLoginHandler, RolesHandler, RoleHandler, UsersHandler,\
#     UserHandler, PermissionsHandler, PermissionHandler, GroupsHandler, GroupHandler,\
#     UserPermissionsHandler, IndexesHandler, IndexHandler, DashboardHandler, UserGroupsHandler, \
#     QuizsHandler

# from handlers.jobs.makejob import MakeJob
# from handlers.jobs.loadjob import LoadJob
# from handlers.jobs.checkjob import CheckJob
# from handlers.jobs.getresult import GetResult
# from handlers.jobs.saveotrest import SaveOtRest
# from handlers.service.makerolemodel import MakeRoleModel
# from handlers.service.makedatamodels import MakeDataModels
# from handlers.service.pingpong import PingPong

from jobs_manager.manager import JobsManager
from task_scheduler.tasks import DbTasksSchduler

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, Open Technologies 98"
__credits__ = ["Anton Khromov"]
__license__ = ""
__version__ = "0.15.4"
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
    user_conf = dict(config['user'])
    pool_conf = dict(config['db_pool_conf'])

    # # # # # # # # # # # # # # # # # # # # # # # # # #

    logger = set_logger(config['general'].get('level', 'INFO'), './logs/otsimplerest.log', 'osr')
    logger.info('DB configuration: %s' % db_conf)
    logger.info('MEM configuration: %s' % mem_conf)

    db_pool = ThreadedConnectionPool(int(pool_conf['min_size']), int(pool_conf['max_size']), **db_conf)

    # Create jobs manager instance and start it
    manager = JobsManager(db_conn_pool=db_pool, mem_conf=mem_conf, disp_conf=disp_conf,
                          resolver_conf=resolver_conf, user_conf=user_conf)
    manager.start()

    # Create and start task scheduler
    scheduler = DbTasksSchduler(db_conn_pool=db_pool)
    scheduler.start()

    # Set TORNADO application with custom handlers.
    application = tornado.web.Application([
        # (r'/ping', PingPong),
        # (r'/checkjob', CheckJob, {"manager": manager}),
        # (r'/getresult', GetResult, {"mem_conf": mem_conf, "static_conf": static_conf}),
        # (r'/makejob', MakeJob, {"manager": manager}),
        # (r'/loadjob', LoadJob, {"manager": manager}),
        # (r'/otrest', SaveOtRest, {"db_conn_pool": db_pool, "mem_conf": mem_conf}),
        # (r'/makerolemodel', MakeRoleModel, {"db_conn_pool": db_pool}),
        # (r'/makedatamodels', MakeDataModels, {"db_conn_pool": db_pool}),

        (r'/api/auth/login', AuthLoginHandler, {"db_conn_pool": db_pool}),

        (r'/api/users', UsersHandler, {"db_conn_pool": db_pool}),
        (r'/api/user', UserHandler, {"db_conn_pool": db_pool}),
        (r'/api/user/groups', UserGroupsHandler, {"db_conn_pool": db_pool}),
        (r'/api/user/permissions', UserPermissionsHandler, {"db_conn_pool": db_pool}),
        (r'/api/user/dashboards', UserDashboardsHandler, {"db_conn_pool": db_pool}),

        (r'/api/groups', GroupsHandler, {"db_conn_pool": db_pool}),
        (r'/api/group', GroupHandler, {"db_conn_pool": db_pool}),
        (r'/api/group/dashboards', GroupDashboardsHandler, {"db_conn_pool": db_pool}),

        (r'/api/roles', RolesHandler, {"db_conn_pool": db_pool}),
        (r'/api/role', RoleHandler, {"db_conn_pool": db_pool}),

        (r'/api/permissions', PermissionsHandler, {"db_conn_pool": db_pool}),
        (r'/api/permission', PermissionHandler, {"db_conn_pool": db_pool}),

        (r'/api/indexes', IndexesHandler, {"db_conn_pool": db_pool}),
        (r'/api/index', IndexHandler, {"db_conn_pool": db_pool}),

        (r'/api/dashs', DashboardsHandler, {"db_conn_pool": db_pool}),
        (r'/api/dash/save', DashboardHandler, {"db_conn_pool": db_pool}),
        (r'/api/dash/load', DashboardHandler, {"db_conn_pool": db_pool}),

        (r'/qapi/quizs', QuizsHandler, {"db_conn_pool": db_pool}),
        (r'/qapi/quiz', QuizHandler, {"db_conn_pool": db_pool}),
        (r'/qapi/quiz/new', QuizHandler, {"db_conn_pool": db_pool})
    ],
        cookie_secret='57ed6cf3-b908-47ca-a3de-88a76aa794cb',
        login_url=r'/api/auth/login',
        debug=True
    )

    logger.info('Starting server')

    # Start application.
    application.listen(50000)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()
