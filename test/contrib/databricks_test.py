"""
Tests for the Databricks integration wrappers
"""

import unittest
import mock
import luigi.contrib.databricks as lcd


class ClusterAutoscale(lcd.CreateDatabricksClusterTask):
    """ Check cluster config for autoscaling """

    min_workers = 1
    max_workers = 2


class ClusterSpecifyWorkers(lcd.CreateDatabricksClusterTask):
    """ Check cluster config for static worker count """

    num_workers = 2
    cluster_params = {
        'aws_attributes': {},
        'spark_conf': {}
    }


class RunNewCluster(lcd._DatabricksJobBaseTask):
    """ Check config for new cluster job """

    num_workers = 1
    run_name = 'test'
    job_libraries = ['testlib']
    timeout_seconds = 6000


class TestDatabricksClusterConfigs(unittest.TestCase):
    """ Tests for Databricks cluster configuration """

    def test_autoscale_config(self):
        """ Test Databricks autoscale config set correctly """
        cluster_conf = ClusterAutoscale()._cluster_conf()
        self.assertTrue(cluster_conf['autoscale']['min_workers'] == 1)
        self.assertTrue(cluster_conf['autoscale']['max_workers'] == 2)

    def test_staticworkers_config(self):
        """ Test Databricks static worker config set correctly """
        cluster_conf = ClusterSpecifyWorkers()._cluster_conf()
        self.assertTrue(cluster_conf['num_workers'] == 2)

    def test_clusterparams_config(self):
        """ Test Databricks cluster parameters set correctly """
        cluster_conf = ClusterSpecifyWorkers()._cluster_conf()
        self.assertTrue(cluster_conf['aws_attributes'] == {})
        self.assertTrue(cluster_conf['spark_conf'] == {})


class TestDatabricksJobFunctions(unittest.TestCase):
    """ Test for Databricks Job functions """

    def test_job_config(self):
        """ Test Databricks RunsSubmit config set """
        job = RunNewCluster().create_runsubmit()
        self.assertTrue('new_cluster' in job)
        self.assertTrue(job['new_cluster']['num_workers'] == 1)
        self.assertTrue(job['run_name'] == 'test')

    @mock.patch('luigi.contrib.databricks._DatabricksJobBaseTask.db_request')
    def test_job_runssubmit(self, mock_req):
        """ Test Databricks RunsSubmit returns job_id """
        mock_req.return_value.json.return_value = {'run_id': 1234}
        job = RunNewCluster().create_runsubmit()
        run_id = RunNewCluster().runssubmit(job)
        self.assertTrue(run_id == 1234)

    @mock.patch('luigi.contrib.databricks._DatabricksJobBaseTask.get_run')
    def test_job_state_pending(self, mock_get):
        """ Test Databricks Job Status PENDING """
        mock_get.return_value = {
            'state': {
                'life_cycle_state': 'PENDING',
                'state_message': 'test message'
            }
        }
        status = RunNewCluster().parse_run_status()
        self.assertFalse(status)

    @mock.patch('luigi.contrib.databricks._DatabricksJobBaseTask.get_run')
    def test_job_state_running(self, mock_get):
        """ Test Databricks Job Status RUNNING """
        mock_get.return_value = {
            'state': {
                'life_cycle_state': 'RUNNING',
                'state_message': 'test message'
            }
        }
        status = RunNewCluster().parse_run_status()
        self.assertFalse(status)

    @mock.patch('luigi.contrib.databricks._DatabricksJobBaseTask.get_run')
    def test_job_state_terminating(self, mock_get):
        """ Test Databricks Job Status TERMINATING """
        mock_get.return_value = {
            'state': {
                'life_cycle_state': 'TERMINATING',
                'state_message': 'test message'
            }
        }
        status = RunNewCluster().parse_run_status()
        self.assertFalse(status)

    @mock.patch('luigi.contrib.databricks._DatabricksJobBaseTask.get_run')
    def test_job_state_term_success(self, mock_get):
        """ Test Databricks Job Status TERMINATED SUCCESS """
        mock_get.return_value = {
            'state': {
                'life_cycle_state': 'TERMINATED',
                'state_message': 'test message',
                'result_state': 'SUCCESS'
            }
        }
        status = RunNewCluster().parse_run_status()
        self.assertTrue(status)
