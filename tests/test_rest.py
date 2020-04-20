import unittest
from configparser import ConfigParser

import sys
sys.path.append('../ot_simple_rest')

from ot_simple_rest.handlers.jobs.db_connector import PostgresConnector

# from rest.checkjob_tester import CheckjobTester
# from rest.makejob_tester import MakejobTester
# from rest.getresult_tester import GetresultTester
# from rest.makerolemodel_tester import MakeRoleModelTester
from rest.eva_tester import EvaTester

from psycopg2.pool import ThreadedConnectionPool

spl = '| ot ttl=60 | makeresults count=10 | simple'

config = ConfigParser()
config.read('tests/rest/test_rest.conf')

pool = ThreadedConnectionPool(2, 4, **dict(config['db_conf']))
eva_pool = ThreadedConnectionPool(2, 4, **dict(config['eva_db_conf']))
db = PostgresConnector(pool)
eva_db = PostgresConnector(eva_pool)


# class TestCheckJob(unittest.TestCase):
#     """
#     Test suite for /api/checkjob OT_REST endpoint.
#     Testing for different job statuses:
#     - job not created
#     - status is 'new'
#     - status is 'running'
#     - status is 'finished'
#     - status is 'failed'
#     - status is 'canceled'
#     """
#
#     tester = CheckjobTester(db, dict(config['rest_conf']))
#     tester.set_query(spl)
#
#     def test__no_job(self):
#         self.assertTrue(self.tester.test__no_job())
#
#     def test__new(self):
#         self.assertTrue(self.tester.test__new())
#
#     def test__running(self):
#         self.assertTrue(self.tester.test__running())
#
#     def test__finished(self):
#         self.assertTrue(self.tester.test__finished())
#
#     def test__finished_nocache(self):
#         self.assertTrue(self.tester.test__finished_nocache())
#
#     def test__failed(self):
#         self.assertTrue(self.tester.test__failed())
#
#     def test__canceled(self):
#         self.assertTrue(self.tester.test__canceled())
#
#
# class TestMakeJob(unittest.TestCase):
#     """
#     Test suite for /api/makejob OT_REST endpoint.
#     Testing for different job statuses:
#     - job not created
#     - status is 'new'
#     - status is 'running'
#     - status is 'finished'
#     - status is 'failed'
#     - status is 'external'
#     - status is 'canceled'
#     """
#     tester = MakejobTester(db, dict(config['rest_conf']))
#     tester.set_query(spl)
#
#     def test__no_job(self):
#         self.assertTrue(self.tester.test__no_job())
#
#     def test__new(self):
#         self.assertTrue(self.tester.test__new())
#
#     def test__running(self):
#         self.assertTrue(self.tester.test__running())
#
#     def test__finished(self):
#         self.assertTrue(self.tester.test__finished())
#
#     def test__finished_expired(self):
#         self.assertTrue(self.tester.test__finished_expired())
#
#     def test__failed(self):
#         self.assertTrue(self.tester.test__failed())
#
#     def test__canceled(self):
#         self.assertTrue(self.tester.test__canceled())
#
#     def test__external(self):
#         self.assertTrue(self.tester.test__external())
#
#
# class TestMakeRoleModel(unittest.TestCase):
#     """
#     Test suite for /api/makerolemodel OT_REST endpoint.
#     Test cases:
#     - create role model
#     """
#     tester = MakeRoleModelTester(db, dict(config['rest_conf']))
#
#     def test__create_model(self):
#         self.assertTrue(self.tester.create_model())
#
#
# class TestGetResult(unittest.TestCase):
#     """
#     Test suite for /api/getresult OT_REST endpoint.
#     Test cases:
#     - returns list of data urls
#     """
#     tester = GetresultTester(dict(config['rest_conf']),
#                              dict(config['mem_conf']),
#                              dict(config['static']))
#
#     def test__get_result(self):
#         self.assertTrue(self.tester.test__getresult())


class TestEva(unittest.TestCase):
    """
    Test suite for EVA OT_REST endpoints.
    Test cases:
    - authorisation
    - create/get/delete/edit user
    - get users list
    - create/get/delete/edit role
    - get roles list
    """
    tester = EvaTester(dict(config['rest_conf']), eva_db)

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


if __name__ == '__main__':
    try:
        unittest.main(verbosity=2)
    except Exception as err:
        print(err)
        exit(1)
