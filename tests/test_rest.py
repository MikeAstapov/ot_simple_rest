import unittest
from configparser import ConfigParser

import sys
sys.path.append('/opt/ot_simple_rest')

from handlers.jobs.db_connector import PostgresConnector

from tests.rest.checkjob_tester import CheckjobTester
from tests.rest.makejob_tester import MakejobTester
from tests.rest.makerolemodel_tester import MakeRoleModelTester

spl = '| ot ttl=60 | search index="pprbappcore_business" cx-communication.availability.name=* ' \
      '| stats values(host) as hosts | mvexpand hosts | simple'

config = ConfigParser()
config.read('test_rest.conf')


class TestCheckJob(unittest.TestCase):
    """
    Test suite for /checkjob OT_REST endpoint.
    Testing for different job statuses:
    - job not created
    - status is 'new'
    - status is 'running'
    - status is 'finished'
    - status is 'failed'
    - status is 'canceled'
    """

    db = PostgresConnector(dict(config['db_conf']))
    tester = CheckjobTester(db, dict(config['rest_conf']))
    tester.set_query(spl)

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
    Test suite for /makejob OT_REST endpoint.
    Testing for different job statuses:
    - job not created
    - status is 'new'
    - status is 'running'
    - status is 'finished'
    - status is 'failed'
    - status is 'external'
    - status is 'canceled'
    """

    db = PostgresConnector(dict(config['db_conf']))
    tester = MakejobTester(db, dict(config['rest_conf']))
    tester.set_query(spl)

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
    Test suite for /makerolemodel OT_REST endpoint.
    Test cases:
    - create role model
    """
    db = PostgresConnector(dict(config['db_conf']))
    tester = MakeRoleModelTester(db, dict(config['rest_conf']))

    def test__create_model(self):
        self.assertTrue(self.tester.create_model())


if __name__ == '__main__':
    unittest.main()
