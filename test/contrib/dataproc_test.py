"""This is an integration test for the Dataproc-luigi binding.

This test requires credentials that can access GCS & access to a bucket below.
Follow the directions in the gcloud tools to set up local credentials.
"""
import unittest

try:
    import google.auth
    from luigi.contrib import dataproc
    from googleapiclient import discovery

    default_credentials, _ = google.auth.default()
    default_client = discovery.build('dataproc', 'v1', cache_discovery=False, credentials=default_credentials)
    dataproc.set_dataproc_client(default_client)
except ImportError:
    raise unittest.SkipTest('Unable to load google cloud dependencies')

import luigi
import os
import time
import pytest

# In order to run this test, you should set these to your GCS project.
# Unfortunately there's no mock
PROJECT_ID = os.environ.get('DATAPROC_TEST_PROJECT_ID', 'your_project_id_here')
CLUSTER_NAME = os.environ.get('DATAPROC_TEST_CLUSTER', 'unit-test-cluster')
REGION = os.environ.get('DATAPROC_REGION', 'global')
IMAGE_VERSION = '1-0'


class _DataprocBaseTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


@pytest.mark.gcloud
class DataprocTaskTest(_DataprocBaseTestCase):

    def test_1_create_cluster(self):
        success = luigi.run(['--local-scheduler',
                             '--no-lock',
                             'CreateDataprocClusterTask',
                             '--gcloud-project-id=' + PROJECT_ID,
                             '--dataproc-cluster-name=' + CLUSTER_NAME])
        self.assertTrue(success)

    def test_2_create_cluster_should_notice_existing_cluster_and_return_immediately(self):
        job_start = time.time()
        success = luigi.run(['--local-scheduler',
                             '--no-lock',
                             'CreateDataprocClusterTask',
                             '--gcloud-project-id=' + PROJECT_ID,
                             '--dataproc-cluster-name=' + CLUSTER_NAME])
        self.assertTrue(success)
        self.assertLess(time.time() - job_start, 3)

    def test_3_submit_minimal_job(self):
        # The job itself will fail because the job files don't exist
        # We don't care, because then we would be testing spark
        # We care the job was submitted correctly, so that's what we test

        luigi.run(['--local-scheduler',
                   '--no-lock',
                   'DataprocSparkTask',
                   '--gcloud-project-id=' + PROJECT_ID,
                   '--dataproc-cluster-name=' + CLUSTER_NAME,
                   '--main-class=my.MinimalMainClass'])

        response = dataproc.get_dataproc_client().projects().regions().jobs() \
            .list(projectId=PROJECT_ID, region=REGION, clusterName=CLUSTER_NAME).execute()
        lastJob = response['jobs'][0]['sparkJob']

        self.assertEqual(lastJob['mainClass'], "my.MinimalMainClass")

    def test_4_submit_spark_job(self):
        # The job itself will fail because the job files don't exist
        # We don't care, because then we would be testing spark
        # We care the job was submitted correctly, so that's what we test

        luigi.run(['--local-scheduler',
                   '--no-lock',
                   'DataprocSparkTask',
                   '--gcloud-project-id=' + PROJECT_ID,
                   '--dataproc-cluster-name=' + CLUSTER_NAME,
                   '--main-class=my.MainClass',
                   '--jars=one.jar,two.jar',
                   '--job-args=foo,bar'])

        response = dataproc.get_dataproc_client().projects().regions().jobs() \
            .list(projectId=PROJECT_ID, region=REGION, clusterName=CLUSTER_NAME).execute()
        lastJob = response['jobs'][0]['sparkJob']

        self.assertEqual(lastJob['mainClass'], "my.MainClass")
        self.assertEqual(lastJob['jarFileUris'], ["one.jar", "two.jar"])
        self.assertEqual(lastJob['args'], ["foo", "bar"])

    def test_5_submit_pyspark_job(self):
        # The job itself will fail because the job files don't exist
        # We don't care, because then we would be testing pyspark
        # We care the job was submitted correctly, so that's what we test

        luigi.run(['--local-scheduler',
                   '--no-lock',
                   'DataprocPysparkTask',
                   '--gcloud-project-id=' + PROJECT_ID,
                   '--dataproc-cluster-name=' + CLUSTER_NAME,
                   '--job-file=main_job.py',
                   '--extra-files=extra1.py,extra2.py',
                   '--job-args=foo,bar'])

        response = dataproc.get_dataproc_client().projects().regions().jobs()\
            .list(projectId=PROJECT_ID, region=REGION, clusterName=CLUSTER_NAME).execute()
        lastJob = response['jobs'][0]['pysparkJob']

        self.assertEqual(lastJob['mainPythonFileUri'], "main_job.py")
        self.assertEqual(lastJob['pythonFileUris'], ["extra1.py", "extra2.py"])
        self.assertEqual(lastJob['args'], ["foo", "bar"])

    def test_6_delete_cluster(self):
        success = luigi.run(['--local-scheduler',
                             '--no-lock',
                             'DeleteDataprocClusterTask',
                             '--gcloud-project-id=' + PROJECT_ID,
                             '--dataproc-cluster-name=' + CLUSTER_NAME])
        self.assertTrue(success)

    def test_7_delete_cluster_should_return_immediately_if_no_cluster(self):
        job_start = time.time()
        success = luigi.run(['--local-scheduler',
                             '--no-lock',
                             'DeleteDataprocClusterTask',
                             '--gcloud-project-id=' + PROJECT_ID,
                             '--dataproc-cluster-name=' + CLUSTER_NAME])
        self.assertTrue(success)
        self.assertLess(time.time() - job_start, 3)

    def test_8_create_cluster_image_version(self):
        success = luigi.run(['--local-scheduler',
                             '--no-lock',
                             'CreateDataprocClusterTask',
                             '--gcloud-project-id=' + PROJECT_ID,
                             '--dataproc-cluster-name=' + CLUSTER_NAME + '-' + IMAGE_VERSION,
                             '--image-version=1.0'])
        self.assertTrue(success)

    def test_9_delete_cluster_image_version(self):
        success = luigi.run(['--local-scheduler',
                             '--no-lock',
                             'DeleteDataprocClusterTask',
                             '--gcloud-project-id=' + PROJECT_ID,
                             '--dataproc-cluster-name=' + CLUSTER_NAME + '-' + IMAGE_VERSION])
        self.assertTrue(success)
