import unittest
from configparser import ConfigParser
from handlers.jobs.db_connector import PostgresConnector

from rest.checkjob_tester import CheckjobTester
from rest.makejob_tester import MakejobTester
from rest.getresult_tester import GetresultTester
from rest.makerolemodel_tester import MakeRoleModelTester
from rest.eva_tester import EvaTester
from rest.quizs_tester import QuizsTester

from psycopg2.pool import ThreadedConnectionPool


class TestCheckJob(unittest.TestCase):
    """
    Test suite for /api/checkjob OT_REST endpoint.
    Testing for different job statuses:
    - job not created
    - status is 'new'
    - status is 'running'
    - status is 'finished'
    - status is 'failed'
    - status is 'canceled'
    """
    config = ConfigParser()
    config.add_section('rest_conf')
    config.set('rest_conf', 'host', 'localhost')
    config.set('rest_conf', 'port', '50000')
    config.add_section('db_conf')
    config.set('db_conf', 'database', 'test_dispatcher')
    config.set('db_conf', 'user', 'tester')
    config.set('db_conf', 'password', 'password')
    config.set('db_conf', 'host', 'localhost')

    otl = '| ot ttl=60 | makeresults count=10 | simple'
    pool = ThreadedConnectionPool(2, 4, **dict(config['db_conf']))
    db = PostgresConnector(pool)

    tester = CheckjobTester(db, dict(config['rest_conf']))
    tester.set_query(otl)

    def test__no_job(self):
        self.assertTrue(self.tester.test__no_job())

    def test__new(self):
        self.assertTrue(self.tester.test__new())

    def test__running(self):
        self.assertTrue(self.tester.test__running())

    def test__finished(self):
        self.assertTrue(self.tester.test__finished())

    def test__finished_nocache(self):
        self.assertTrue(self.tester.test__finished_nocache())

    def test__failed(self):
        self.assertTrue(self.tester.test__failed())

    def test__canceled(self):
        self.assertTrue(self.tester.test__canceled())


class TestMakeJob(unittest.TestCase):
    """
    Test suite for /api/makejob OT_REST endpoint.
    Testing for different job statuses:
    - job not created
    - status is 'new'
    - status is 'running'
    - status is 'finished'
    - status is 'failed'
    - status is 'external'
    - status is 'canceled'
    """
    config = ConfigParser()
    config.add_section('rest_conf')
    config.set('rest_conf', 'host', 'localhost')
    config.set('rest_conf', 'port', '50000')
    config.add_section('db_conf')
    config.set('db_conf', 'database', 'test_dispatcher')
    config.set('db_conf', 'user', 'tester')
    config.set('db_conf', 'password', 'password')
    config.set('db_conf', 'host', 'localhost')

    otl = '| ot ttl=60 | makeresults count=10 | simple'
    pool = ThreadedConnectionPool(2, 4, **dict(config['db_conf']))
    db = PostgresConnector(pool)

    tester = MakejobTester(db, dict(config['rest_conf']))
    tester.set_query(otl)

    def test__no_job(self):
        self.assertTrue(self.tester.test__no_job())

    def test__new(self):
        self.assertTrue(self.tester.test__new())

    def test__running(self):
        self.assertTrue(self.tester.test__running())

    def test__finished(self):
        self.assertTrue(self.tester.test__finished())

    def test__finished_expired(self):
        self.assertTrue(self.tester.test__finished_expired())

    def test__failed(self):
        self.assertTrue(self.tester.test__failed())

    def test__canceled(self):
        self.assertTrue(self.tester.test__canceled())

    def test__external(self):
        self.assertTrue(self.tester.test__external())


class TestMakeRoleModel(unittest.TestCase):
    """
    Test suite for /api/makerolemodel OT_REST endpoint.
    Test cases:
    - create role model
    """
    config = ConfigParser()
    config.add_section('rest_conf')
    config.set('rest_conf', 'host', 'localhost')
    config.set('rest_conf', 'port', '50000')
    config.add_section('db_conf')
    config.set('db_conf', 'database', 'test_dispatcher')
    config.set('db_conf', 'user', 'tester')
    config.set('db_conf', 'password', 'password')
    config.set('db_conf', 'host', 'localhost')

    pool = ThreadedConnectionPool(2, 4, **dict(config['db_conf']))
    db = PostgresConnector(pool)

    tester = MakeRoleModelTester(db, dict(config['rest_conf']))

    def test__create_model(self):
        self.assertTrue(self.tester.create_model())


class TestGetResult(unittest.TestCase):
    """
    Test suite for /api/getresult OT_REST endpoint.
    Test cases:
    - returns list of data urls
    """
    config = ConfigParser()
    config.add_section('rest_conf')
    config.set('rest_conf', 'host', 'localhost')
    config.set('rest_conf', 'port', '50000')
    config.add_section('mem_conf')
    config.set('mem_conf', 'path', '/tmp/caches')
    config.add_section('static')
    config.set('static', 'use_nginx', 'True')
    config.set('static', 'base_url', 'cache/{}')

    tester = GetresultTester(dict(config['rest_conf']),
                             dict(config['mem_conf']),
                             dict(config['static']))

    def test__get_result(self):
        self.assertTrue(self.tester.test__getresult())


class TestEva(unittest.TestCase):
    """
    Test suite for EVA OT_REST endpoints.
    Test cases:
    - authorisation
    - create/get/delete/edit user
    - get users list
    - create/get/delete/edit role
    - get roles list
    - create/get/delete/edit group
    - get groups list
    - create/get/delete/edit permission
    - get permissions list
    - create/get/delete/edit index
    - get indexes list
    - create/get/delete/edit dash
    - get dashs list
    """
    config = ConfigParser()
    config.add_section('rest_conf')
    config.set('rest_conf', 'host', 'localhost')
    config.set('rest_conf', 'port', '50000')
    config.add_section('eva_db_conf')
    config.set('eva_db_conf', 'database', 'test_eva')
    config.set('eva_db_conf', 'user', 'tester')
    config.set('eva_db_conf', 'password', 'password')
    config.set('eva_db_conf', 'host', 'localhost')
    config.add_section('static')
    config.set('static', 'static_path', '/opt/otp/static/')

    eva_pool = ThreadedConnectionPool(2, 4, **dict(config['eva_db_conf']))
    eva_db = PostgresConnector(eva_pool)
    tester = EvaTester(dict(config), eva_db)

    def test__auth(self):
        self.assertTrue(self.tester.test__auth())

    def test__create_user(self):
        self.assertTrue(self.tester.test__create_user())

    def test__delete_user(self):
        self.assertTrue(self.tester.test__delete_user())

    def test__get_user(self):
        self.assertTrue(self.tester.test__get_user())

    def test__edit_user(self):
        self.assertTrue(self.tester.test__edit_user())

    def test__get_users_list(self):
        self.assertTrue(self.tester.test__get_users_list())

    def test__create_role(self):
        self.assertTrue(self.tester.test__create_role())

    def test__delete_role(self):
        self.assertTrue(self.tester.test__delete_role())

    def test__edit_role(self):
        self.assertTrue(self.tester.test__edit_role())

    def test__get_role(self):
        self.assertTrue(self.tester.test__get_role())

    def test__get_roles_list(self):
        self.assertTrue(self.tester.test__get_roles_list())

    def test__create_group(self):
        self.assertTrue(self.tester.test__create_group())

    def test__delete_group(self):
        self.assertTrue(self.tester.test__delete_group())

    def test__edit_group(self):
        self.assertTrue(self.tester.test__edit_group())

    def test__get_group(self):
        self.assertTrue(self.tester.test__get_group())

    def test__get_groups_list(self):
        self.assertTrue(self.tester.test__get_groups_list())

    def test__create_permission(self):
        self.assertTrue(self.tester.test__create_permission())

    def test__delete_permission(self):
        self.assertTrue(self.tester.test__delete_permission())

    def test__edit_permission(self):
        self.assertTrue(self.tester.test__edit_permission())

    def test__get_permission(self):
        self.assertTrue(self.tester.test__get_permission())

    def test__get_permissions_list(self):
        self.assertTrue(self.tester.test__get_permissions_list())

    def test__create_index(self):
        self.assertTrue(self.tester.test__create_index())

    def test__delete_index(self):
        self.assertTrue(self.tester.test__delete_index())

    def test__edit_index(self):
        self.assertTrue(self.tester.test__edit_index())

    def test__get_index(self):
        self.assertTrue(self.tester.test__get_index())

    def test__get_indexes_list(self):
        self.assertTrue(self.tester.test__get_indexes_list())

    def test__create_dash(self):
        self.assertTrue(self.tester.test__create_dash())

    def test__delete_dash(self):
        self.assertTrue(self.tester.test__delete_dash())

    def test__edit_dash(self):
        self.assertTrue(self.tester.test__edit_dash())

    def test__get_dash(self):
        self.assertTrue(self.tester.test__get_dash())

    def test__get_dashs_list(self):
        self.assertTrue(self.tester.test__get_dashs_list())

    def test__get_user_permissions(self):
        self.assertTrue(self.tester.test__get_user_permissions())

    def test__get_user_groups(self):
        self.assertTrue(self.tester.test__get_user_groups())

    def test__get_user_dashs(self):
        self.assertTrue(self.tester.test__get_user_dashs())

    def test__get_group_dashs(self):
        self.assertTrue(self.tester.test__get_group_dashs())

    def test__import_dash_single(self):
        self.assertTrue(self.tester.test__import_dash_single())

    def test__import_dash_multi(self):
        self.assertTrue(self.tester.test__import_dash_multi())

    def test__export_dash_single(self):
        self.assertTrue(self.tester.test__export_dash_single())

    def test__export_dash_multi(self):
        self.assertTrue(self.tester.test__export_dash_multi())

    def test__import_dash_group_single(self):
        self.assertTrue(self.tester.test__import_dash_group_single())

    def test__import_dash_group_multi(self):
        self.assertTrue(self.tester.test__import_dash_group_multi())

    def test__export_dash_group_single(self):
        self.assertTrue(self.tester.test__export_dash_group_single())

    def test__export_dash_group_multi(self):
        self.assertTrue(self.tester.test__export_dash_group_multi())


class TestQuizs(unittest.TestCase):
    """
    Test suite for /qapi/quiz(s) OT_REST endpoint.
    Test cases:
    - get quiz
    - get quizs list
    - create quiz
    - delete quiz
    - edit quiz
    - get quiz questions
    - fill quiz
    - get filled quizs
    """
    config = ConfigParser()
    config.add_section('rest_conf')
    config.set('rest_conf', 'host', 'localhost')
    config.set('rest_conf', 'port', '50000')
    config.add_section('eva_db_conf')
    config.set('eva_db_conf', 'database', 'test_eva')
    config.set('eva_db_conf', 'user', 'tester')
    config.set('eva_db_conf', 'password', 'password')
    config.set('eva_db_conf', 'host', 'localhost')

    eva_pool = ThreadedConnectionPool(2, 4, **dict(config['eva_db_conf']))
    eva_db = PostgresConnector(eva_pool)

    tester = QuizsTester(dict(config['rest_conf']), eva_db)

    def test__create_quiz(self):
        self.assertTrue(self.tester.test__create_quiz())

    def test__delete_quiz(self):
        self.assertTrue(self.tester.test__delete_quiz())

    def test__edit_quiz(self):
        self.assertTrue(self.tester.test__edit_quiz())

    def test__get_quiz(self):
        self.assertTrue(self.tester.test__get_quiz())

    def test__get_quizs_list(self):
        self.assertTrue(self.tester.test__get_quizs_list())

    def test__get_quiz_questions(self):
        self.assertTrue(self.tester.test__get_quiz_questions())

    def test__fill_quiz(self):
        self.assertTrue(self.tester.test__fill_quiz())

    def test__get_filled_quizs(self):
        self.assertTrue(self.tester.test__get_filled_quizs())

    def test__create_catalog(self):
        self.assertTrue(self.tester.test__create_catalog())

    def test__delete_catalog(self):
        self.assertTrue(self.tester.test__delete_catalog())

    def test__edit_catalog(self):
        self.assertTrue(self.tester.test__edit_catalog())

    def test__get_catalog(self):
        self.assertTrue(self.tester.test__get_catalog())

    def test__get_catalogs_list(self):
        self.assertTrue(self.tester.test__get_catalogs_list())

