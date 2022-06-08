#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ot_simple_rest.py

import logging.config
import os
import sys
from configparser import ConfigParser

import tornado.ioloop

from psycopg2.pool import ThreadedConnectionPool

from handlers.eva.auth import AuthLoginHandler
from handlers.eva.logs import LogsHandler
from handlers.eva.dashs import DashboardHandler, DashboardsHandler, DashExportHandler, \
    DashImportHandler, GroupExportHandler, GroupImportHandler, DashByNameHandler
from handlers.eva.quizs import QuizsHandler, QuizHandler, QuizQuestionsHandler, QuizFilledHandler, \
    FilledQuizExportHandler, QuizExportJsonHandler, QuizImportJsonHandler, CatalogsListHandler, CatalogHandler
from handlers.eva.role_model import UserHandler, UsersHandler, RoleHandler, RolesHandler, \
    PermissionsHandler, PermissionHandler, GroupsHandler, GroupHandler, UserPermissionsHandler, \
    IndexesHandler, IndexHandler, UserGroupsHandler, UserDashboardsHandler, GroupDashboardsHandler, UserSettingHandler
from handlers.eva.papers import PaperLoadHandler, PapersHandler, PaperHandler
from handlers.eva.svg_load import SvgLoadHandler

from handlers.eva.theme import ThemeListHandler, ThemeGetHandler, ThemeHandler
from handlers.eva.timelines import GetTimelines
from handlers.eva.interesting_fields import GetInterestingFields

from handlers.jobs.makejob import MakeJob
from handlers.jobs.loadjob import LoadJob
from handlers.jobs.checkjob import CheckJob
from handlers.jobs.getresult import GetResult
from handlers.jobs.saveotrest import SaveOtRest
from handlers.service.makerolemodel import MakeRoleModel
from handlers.service.makedatamodels import MakeDataModels
from handlers.service.pingpong import PingPong

from jobs_manager.manager import JobsManager
from task_scheduler.tasks import DbTasksSchduler

__author__ = "Andrey Starchenkov"
__copyright__ = "Copyright 2019, ISG Neuro"
__credits__ = ["Anton Khromov"]
__license__ = ""
__version__ = "1.11.0"
__maintainer__ = "Egor Lukyanov"
__email__ = "astarchenkov@ot.ru"
__status__ = "Production"

from utils.tornado_mod import Tornado


def set_logger(loglevel, logfile, logger_name):
    levels = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG
    }

    log_dict = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': "%(asctime)s %(levelname)-s PID=%(process)d %(module)s:%(lineno)d "
                          "func=%(funcName)s - %(message)s"
            },
            'with_hid': {
                'format': "%(asctime)s %(levelname)-s PID=%(process)d HID=%(hid)s %(module)s:%(lineno)d "
                          "func=%(funcName)s - %(message)s"
            }
        },
        'handlers': {
            'file_handler_standard': {
                'filename': logfile,
                'level': levels[loglevel],
                'class': 'logging.FileHandler',
                'formatter': 'standard'
            },
            'file_handler_with_hid': {
                'filename': logfile,
                'level': levels[loglevel],
                'class': 'logging.FileHandler',
                'formatter': 'with_hid'
            }
        },
        'loggers': {
            'osr': {
                'handlers': ['file_handler_standard'],
                'level': levels[loglevel],
                'propagate': False
            },
            'osr_hid': {
                'handlers': ['file_handler_with_hid'],
                'level': levels[loglevel]
            },
        },
        'root': {
            'handlers': ['file_handler_standard'],
            'level': levels[loglevel]
        }
    }

    logging.config.dictConfig(log_dict)
    logger = logging.getLogger(logger_name)
    return logger


def main():
    """
    Main function with config and starter code. It starts TORNADO server with custom handlers which register, check and
    load Job's results from ramcache to OT.Simple OTP app.
    :return:
    """

    # # # # # # #  Configuration section  # # # # # # #

    # check if you are running script or executable to get right config path
    if getattr(sys, 'frozen', False):
        basedir = os.path.dirname(sys.executable)
    else:
        basedir = os.path.dirname(os.path.abspath(__file__))

    config = ConfigParser()
    config.read(os.path.join(basedir, 'ot_simple_rest.conf'))

    db_conf = dict(config['db_conf'])
    db_conf_eva = dict(config['db_conf_eva'])
    mem_conf = dict(config['mem_conf'])
    disp_conf = dict(config['dispatcher'])
    resolver_conf = dict(config['resolver'])
    static_conf = dict(config['static'])
    user_conf = dict(config['user'])
    pool_conf = dict(config['db_pool_conf'])
    notification_conf = dict(config['notification_triggers']) if 'notification_triggers' in config else dict()
    file_upload_conf = dict(config['file_upload']) if 'file_upload' in config else dict()

    # # # # # # # # # # # # # # # # # # # # # # # # # #

    base_logs_dir = config['general'].get('logs_path', '.')
    if not os.path.exists(base_logs_dir):
        os.makedirs(base_logs_dir)

    logger = set_logger(config['general'].get('level', 'INFO'),
                        os.path.join(base_logs_dir, 'otsimplerest.log'), 'osr')
    logger.info('Version: %s' % __version__)
    logger.info('DB configuration: %s' % db_conf)
    logger.info('MEM configuration: %s' % mem_conf)

    db_pool = ThreadedConnectionPool(int(pool_conf['min_size']), int(pool_conf['max_size']), **db_conf)
    db_pool_eva = ThreadedConnectionPool(int(pool_conf['min_size']), int(pool_conf['max_size']), **db_conf_eva)

    # Create jobs manager instance and start it
    manager = JobsManager(db_conn_pool=db_pool, mem_conf=mem_conf, disp_conf=disp_conf, resolver_conf=resolver_conf)
    manager.start()

    # Create and start task scheduler
    scheduler = DbTasksSchduler(db_conn_pool=db_pool_eva)
    scheduler.start()

    # Set TORNADO application with custom handlers.
    application = Tornado([
        (r'/api/ping', PingPong),
        (r'/api/checkjob', CheckJob, {"manager": manager, "notification_conf": notification_conf,
                                      "db_conn_pool": db_pool}),
        (r'/api/getresult', GetResult, {"mem_conf": mem_conf, "static_conf": static_conf}),
        (r'/api/gettimelines', GetTimelines, {"mem_conf": mem_conf, "static_conf": static_conf,
                                              "notification_conf": notification_conf}),
        (r'/api/getinterestingfields', GetInterestingFields, {"mem_conf": mem_conf, "static_conf": static_conf}),
        (r'/api/makejob', MakeJob, {"db_conn_pool": db_pool_eva, "manager": manager, "user_conf": user_conf}),
        (r'/api/loadjob', LoadJob, {"manager": manager}),
        (r'/api/otrest', SaveOtRest, {"db_conn_pool": db_pool, "mem_conf": mem_conf}),
        (r'/api/makerolemodel', MakeRoleModel, {"db_conn_pool": db_pool}),
        (r'/api/makedatamodels', MakeDataModels, {"db_conn_pool": db_pool}),

        (r'/api/auth/login', AuthLoginHandler, {"db_conn_pool": db_pool_eva}),

        (r'/api/logs/save', LogsHandler, {"db_conn_pool": db_pool_eva,
                                          "logs_path": config['general'].get('logs_path', '.')}),

        (r'/api/users', UsersHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/user', UserHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/user/groups', UserGroupsHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/user/roles', RolesHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/user/permissions', UserPermissionsHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/user/dashs', UserDashboardsHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/user/users', UsersHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/user/indexes', IndexesHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/user/setting', UserSettingHandler, {"db_conn_pool": db_pool_eva}),

        (r'/api/groups', GroupsHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/group', GroupHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/group/export', GroupExportHandler, {"db_conn_pool": db_pool_eva, "static_conf": static_conf}),
        (r'/api/group/import', GroupImportHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/group/dashs', GroupDashboardsHandler, {"db_conn_pool": db_pool_eva}),

        (r'/api/roles', RolesHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/role', RoleHandler, {"db_conn_pool": db_pool_eva}),

        (r'/api/permissions', PermissionsHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/permission', PermissionHandler, {"db_conn_pool": db_pool_eva}),

        (r'/api/indexes', IndexesHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/index', IndexHandler, {"db_conn_pool": db_pool_eva}),

        (r'/api/dashs', DashboardsHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/dash', DashboardHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/dash/export', DashExportHandler, {"db_conn_pool": db_pool_eva, "static_conf": static_conf}),
        (r'/api/dash/import', DashImportHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/dashByName', DashByNameHandler, {"db_conn_pool": db_pool_eva}),

        (r'/api/load/svg', SvgLoadHandler, {"db_conn_pool": db_pool_eva, 'file_upload_conf': file_upload_conf,
                                            'static_conf': static_conf}),

        (r'/qapi/quizs', QuizsHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/quiz', QuizHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/quiz/create', QuizHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/quiz/edit', QuizHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/quiz/delete', QuizHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/quiz/filled', QuizFilledHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/quiz/export', QuizExportJsonHandler, {"db_conn_pool": db_pool_eva,
                                                       "static_conf": static_conf}),
        (r'/qapi/quiz/import', QuizImportJsonHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/quiz/filled/save', QuizFilledHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/quiz/filled/export', FilledQuizExportHandler, {"db_conn_pool": db_pool_eva,
                                                                "static_conf": static_conf}),
        (r'/qapi/quiz/questions', QuizQuestionsHandler, {"db_conn_pool": db_pool_eva}),

        (r'/qapi/catalogs', CatalogsListHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/catalog', CatalogHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/catalog/create', CatalogHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/catalog/edit', CatalogHandler, {"db_conn_pool": db_pool_eva}),
        (r'/qapi/catalog/delete', CatalogHandler, {"db_conn_pool": db_pool_eva}),

        (r'/api/eva/reports/load', PaperLoadHandler, {"db_conn_pool": db_pool_eva,"static_conf": static_conf}),
        (r'/api/eva/reports/getAll', PapersHandler, {"db_conn_pool": db_pool_eva,"static_conf": static_conf}),
        (r'/api/eva/reports/get', PaperHandler, {"db_conn_pool": db_pool_eva,"static_conf": static_conf,"mem_conf": mem_conf}),

        (r'/api/themes', ThemeListHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/theme', ThemeGetHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/theme/create', ThemeHandler, {"db_conn_pool": db_pool_eva}),
        (r'/api/theme/delete', ThemeHandler, {"db_conn_pool": db_pool_eva}),
    ],
        login_url=r'/api/auth/login',

        log_user_activity=False if user_conf.get(
            'log_user_activity', 'False') == 'False' else True  # simple_rest style ^.-
    )

    logger.info('Starting server')

    # Start application.
    try:
        application.listen(50000)
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        pass
    finally:
        tornado.ioloop.IOLoop.current().stop()
        db_pool.closeall()
        db_pool_eva.closeall()


if __name__ == '__main__':
    main()
