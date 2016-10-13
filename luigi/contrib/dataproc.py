"""luigi bindings for Google Dataproc on Google Cloud"""

import os
import time
import logging
import luigi

logger = logging.getLogger('luigi-interface')

_dataproc_client = None

try:
    import httplib2
    import oauth2client.client
    from googleapiclient import discovery
    from googleapiclient.errors import HttpError

    DEFAULT_CREDENTIALS = oauth2client.client.GoogleCredentials.get_application_default()
    _dataproc_client = discovery.build('dataproc', 'v1', credentials=DEFAULT_CREDENTIALS, http=httplib2.Http())
except ImportError:
    logger.warning("Loading Dataproc module without the python packages googleapiclient & oauth2client. \
        This will crash at runtime if Dataproc functionality is used.")


def get_dataproc_client():
    return _dataproc_client


def set_dataproc_client(client):
    global _dataproc_client
    _dataproc_client = client


class _DataprocBaseTask(luigi.Task):
    gcloud_project_id = luigi.Parameter(significant=False, positional=False)
    dataproc_cluster_name = luigi.Parameter(significant=False, positional=False)
    dataproc_region = luigi.Parameter(default="global", significant=False, positional=False)

    dataproc_client = get_dataproc_client()


class DataprocBaseTask(_DataprocBaseTask):
    """
    Base task for running jobs in Dataproc. It is recommended to use one of the tasks specific to your job type.
    Extend this class if you need fine grained control over what kind of job gets submitted to your Dataproc cluster.
    """

    _job = None
    _job_name = None
    _job_id = None

    def submit_job(self, job_config):
        self._job = self.dataproc_client.projects().regions().jobs()\
            .submit(projectId=self.gcloud_project_id, region=self.dataproc_region, body=job_config).execute()
        self._job_id = self._job['reference']['jobId']
        return self._job

    def submit_spark_job(self, jars, main_class, job_args=[]):
        job_config = {"job": {
            "placement": {
                "clusterName": self.dataproc_cluster_name
            },
            "sparkJob": {
                "args": job_args,
                "mainClass": main_class,
                "jarFileUris": jars
            }
        }}
        self.submit_job(job_config)
        self._job_name = os.path.basename(self._job['sparkJob']['mainClass'])
        logger.info("Submitted new dataproc job:{} id:{}".format(self._job_name, self._job_id))
        return self._job

    def submit_pyspark_job(self, job_file, extra_files=[], job_args=[]):
        job_config = {"job": {
            "placement": {
                "clusterName": self.dataproc_cluster_name
            },
            "pysparkJob": {
                "mainPythonFileUri": job_file,
                "pythonFileUris": extra_files,
                "args": job_args
            }
        }}
        self.submit_job(job_config)
        self._job_name = os.path.basename(self._job['pysparkJob']['mainPythonFileUri'])
        logger.info("Submitted new dataproc job:{} id:{}".format(self._job_name, self._job_id))
        return self._job

    def wait_for_job(self):
        if self._job is None:
            raise Exception("You must submit a job before you can wait for it")
        while True:
            job_result = self.dataproc_client.projects().regions().jobs()\
                .get(projectId=self.gcloud_project_id, region=self.dataproc_region, jobId=self._job_id).execute()
            status = job_result['status']['state']
            logger.info("Current dataproc status: {} job:{} id:{}".format(status, self._job_name, self._job_id))
            if status == 'DONE':
                break
            if status == 'ERROR':
                raise Exception(job_result['status']['details'])
            time.sleep(5)


class DataprocSparkTask(DataprocBaseTask):
    """
    Runs a spark jobs on your Dataproc cluster
    """
    main_class = luigi.Parameter()
    jars = luigi.Parameter(default="")
    job_args = luigi.Parameter(default="")

    def run(self):
        self.submit_spark_job(main_class=self.main_class,
                              jars=self.jars.split(",") if self.jars else [],
                              job_args=self.job_args.split(",") if self.job_args else [])
        self.wait_for_job()


class DataprocPysparkTask(DataprocBaseTask):
    """
    Runs a pyspark jobs on your Dataproc cluster
    """
    job_file = luigi.Parameter()
    extra_files = luigi.Parameter(default="")
    job_args = luigi.Parameter(default="")

    def run(self):
        self.submit_pyspark_job(job_file=self.job_file,
                                extra_files=self.extra_files.split(",") if self.extra_files else [],
                                job_args=self.job_args.split(",") if self.job_args else [])
        self.wait_for_job()


class CreateDataprocClusterTask(_DataprocBaseTask):
    """ Task for creating a Dataproc cluster. """

    gcloud_zone = luigi.Parameter(default="europe-west1-c")
    gcloud_network = luigi.Parameter(default="default")

    master_node_type = luigi.Parameter(default="n1-standard-2")
    master_disk_size = luigi.Parameter(default="100")
    worker_node_type = luigi.Parameter(default="n1-standard-2")
    worker_disk_size = luigi.Parameter(default="100")
    worker_normal_count = luigi.Parameter(default="2")
    worker_preemptible_count = luigi.Parameter(default="0")
    image_version = luigi.Parameter(default="")

    def _get_cluster_status(self):
        return self.dataproc_client.projects().regions().clusters()\
            .get(projectId=self.gcloud_project_id, region=self.dataproc_region, clusterName=self.dataproc_cluster_name)\
            .execute()

    def complete(self):
        try:
            self._get_cluster_status()
            return True  # No (404) error so the cluster already exists
        except HttpError as e:
            if e.resp.status == 404:
                return False  # We got a 404 so the cluster doesn't exist yet
            else:
                raise e  # Something's wrong ...

    def run(self):
        base_uri = "https://www.googleapis.com/compute/v1/projects/{}".format(self.gcloud_project_id)
        software_config = {"imageVersion": self.image_version} if self.image_version else {}

        cluster_conf = {
            "clusterName": self.dataproc_cluster_name,
            "projectId": self.gcloud_project_id,
            "config": {
                "configBucket": "",
                "gceClusterConfig": {
                    "networkUri": base_uri + "/global/networks/" + self.gcloud_network,
                    "zoneUri": base_uri + "/zones/" + self.gcloud_zone,
                    "serviceAccountScopes": [
                        "https://www.googleapis.com/auth/cloud-platform"
                    ]
                },
                "masterConfig": {
                    "numInstances": 1,
                    "machineTypeUri": base_uri + "/zones/" + self.gcloud_zone + "/machineTypes/" + self.master_node_type,
                    "diskConfig": {
                        "bootDiskSizeGb": self.master_disk_size,
                        "numLocalSsds": 0
                    }
                },
                "workerConfig": {
                    "numInstances": self.worker_normal_count,
                    "machineTypeUri": base_uri + "/zones/" + self.gcloud_zone + "/machineTypes/" + self.worker_node_type,
                    "diskConfig": {
                        "bootDiskSizeGb": self.worker_disk_size,
                        "numLocalSsds": 0
                    }
                },
                "secondaryWorkerConfig": {
                    "numInstances": self.worker_preemptible_count,
                    "isPreemptible": True
                },
                "softwareConfig": software_config
            }
        }

        self.dataproc_client.projects().regions().clusters()\
            .create(projectId=self.gcloud_project_id, region=self.dataproc_region, body=cluster_conf).execute()

        while True:
            time.sleep(10)
            cluster_status = self._get_cluster_status()
            status = cluster_status['status']['state']
            logger.info("Creating new dataproc cluster: {} status: {}".format(self.dataproc_cluster_name, status))
            if status == 'RUNNING':
                break
            if status == 'ERROR':
                raise Exception(cluster_status['status']['details'])


class DeleteDataprocClusterTask(_DataprocBaseTask):
    """
    Task for deleting a Dataproc cluster.
    One of the uses for this class is to extend it and have it require a Dataproc task that does a calculation and have
    that task extend the cluster creation task. This allows you to create chains where you create a cluster,
    run your job and remove the cluster right away.
    (Store your input and output files in gs://... instead of hdfs://... if you do this).
    """

    def _get_cluster_status(self):
        try:
            return self.dataproc_client.projects().regions().clusters()\
                .get(projectId=self.gcloud_project_id, region=self.dataproc_region,
                     clusterName=self.dataproc_cluster_name, fields="status")\
                .execute()
        except HttpError as e:
            if e.resp.status == 404:
                return None  # We got a 404 so the cluster doesn't exist
            else:
                raise e

    def complete(self): return self._get_cluster_status() is None

    def run(self):
        self.dataproc_client.projects().regions().clusters()\
            .delete(projectId=self.gcloud_project_id, region=self.dataproc_region, clusterName=self.dataproc_cluster_name).execute()

        while True:
            time.sleep(10)
            status = self._get_cluster_status()
            if status is None:
                logger.info("Finished shutting down cluster: {}".format(self.dataproc_cluster_name))
                break
            logger.info("Shutting down cluster: {} current status: {}".format(self.dataproc_cluster_name, status['status']['state']))
