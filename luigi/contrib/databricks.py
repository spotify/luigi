""" Databricks job wrapper for Luigi """

import time
import logging
from json import load

import luigi

LOGGER = logging.getLogger('luigi-interface')

try:
    from requests import request
except ImportError:
    LOGGER.warning("This module requires the python package 'requests'.")


class databricks(luigi.Config):
    """ Get config vars from 'databricks' section in configuration file """

    username = luigi.Parameter(default='')
    password = luigi.Parameter(default='')
    token = luigi.Parameter(default='')
    instance = luigi.Parameter(default='community.cloud.databricks.com')


class _DatabricksBaseTask(luigi.Task):
    """
    Base parameters for DatabricksBaseTask

    A generic
    """

    if databricks().token != '':
        auth_method = 'token'
    else:
        auth_method = 'basic'
        LOGGER.warning('Using basic authentication for Databricks; %s',
                       'it is advised to use a token in shared environments')

    # Adding cluster parameters here for use in either CreateDatabricksClusterTask
    # or ad-hoc creation from _DatabricksJobBaseTask
    cluster_name = luigi.Parameter(default='')
    node_type_id = luigi.Parameter(default='dev-tier-node')
    spark_version = luigi.Parameter(default='3.4.x-scala2.10')
    num_workers = luigi.IntParameter(default=0)
    min_workers = luigi.IntParameter(default=0)
    max_workers = luigi.IntParameter(default=0)
    cluster_params = luigi.DictParameter(default={})

    def _cluster_conf(self):
        """ Create cluster configuration """

        cluster_conf = {
            'cluster_name': self.cluster_name,
            'node_type_id': self.node_type_id,
            'spark_version': self.spark_version
        }

        if self.num_workers > 0:
            cluster_conf['num_workers'] = self.num_workers
        else:
            cluster_conf['autoscale'] = {
                'min_workers': self.min_workers,
                'max_workers': self.max_workers
            }

        cluster_conf.update(self.cluster_params)

        return cluster_conf

    @staticmethod
    def _url_format(uri):
        """ return url """

        return 'https://{instance}/api/2.0/{uri}'.format(
            instance=databricks().instance,
            uri=uri
        )

    def db_request(self, method, uri, params=None, json=None):
        """ Generic request method """

        headers = {}

        if method == 'post':
            headers['Content-Type'] = 'application/json'

        if self.auth_method == 'token':
            headers['Authorization'] = 'Bearer {token}'.format(
                token=databricks().token
            )

            req = request(method,
                          url=self._url_format(uri),
                          params=params,
                          json=json,
                          headers=headers
                          )
        else:
            req = request(method,
                          url=self._url_format(uri),
                          params=params,
                          json=json,
                          headers=headers,
                          auth=(databricks().username, databricks().password)
                          )

        if req.status_code == 400:
            raise Exception(req.json())

        req.raise_for_status()

        return req

    def signal_complete(self, out_data=None):
        """Signal job completion for scheduler and dependent tasks.

         Touching a system file is an easy way to signal completion. example::
         .. code-block:: python

         with self.output().open('w') as output_file:
             output_file.write('')
        """
        pass

    def output(self):
        """
        An output target is necessary for checking job completion unless
        an alternative complete method is defined.

        Example::

            return luigi.LocalTarget(os.path.join('/tmp', 'example'))

        """
        pass


class _DatabricksJobBaseTask(_DatabricksBaseTask):
    """ Base task for Databricks jobs """

    _run_id = None

    cluster_id = luigi.Parameter(default='')
    run_name = luigi.Parameter(default='')
    job_libraries = luigi.ListParameter(default=[])
    timeout_seconds = luigi.IntParameter(default=0)

    def create_runsubmit(self):
        """ Creates a new config for a Runs Submit job """

        runs_submit = {
            'run_name': self.run_name,
            'libraries': self.job_libraries,
            'timeout_seconds': self.timeout_seconds
        }

        if self.cluster_id != '':
            runs_submit['existing_cluster_id'] = self.cluster_id
        else:
            runs_submit['new_cluster'] = self._cluster_conf()

        return runs_submit

    def runssubmit(self, run):
        """ Create a RunsSubmit job """

        req = self.db_request(
            method='post',
            uri='jobs/runs/submit',
            json=run
        )

        self._run_id = req.json()['run_id']
        return self._run_id

    def get_run(self):
        """ return job object """

        if self._run_id is None:
            raise Exception("No job submitted")

        req = self.db_request(
            method='get',
            uri='jobs/runs/get',
            params={
                'run_id': self._run_id
            }
        )

        return req.json()

    def parse_run_status(self):
        """ determine success or failure of a job run """

        run = self.get_run()

        run_state = run['state']['lifecycle_state']
        run_state_msg = run['state']['state_message']

        if run_state in ['PENDING', 'RUNNING', 'TERMINATING']:
            LOGGER.info('Databricks job run %s has status %s',
                        self._run_id, run_state)
            return False
        elif run_state == 'TERMINATED' and run['state']['result_state'] == 'SUCCESS':
            LOGGER.info('Databricks job run %s is SUCCESS', self._run_id)
            return True
        elif 'result_state' in run['state']:
            raise Exception('Databricks job run %s is %s with state %s; %s',
                            self._run_id, run_state, run[
                                'state']['result_state'],
                            run_state_msg)
        else:
            raise Exception('Databricks job run %s is %s; %s',
                            self._run_id, run_state, run_state_msg)

    def wait_for_job(self):
        """ Check job until complete """
        if self._run_id is None:
            raise Exception("No job submitted")
        while True:
            if self.parse_run_status() is True:
                break
            time.sleep(10)


class DatabricksNotebookTask(_DatabricksJobBaseTask):
    """ Databricks Notebook job task """

    notebook_path = luigi.Parameter()
    base_parameters = luigi.DictParameter(default={})

    def run(self):
        """ Run a new Databricks Notebook job task """

        new_rsub = self.create_runsubmit()

        new_rsub['notebook_task'] = {
            'notebook_path': self.notebook_path,
            'base_parameters': self.base_parameters
        }

        self.runssubmit(new_rsub)

        self.wait_for_job()

        self.signal_complete()


class DatabricksSparkPythonTask(_DatabricksJobBaseTask):
    """ Databricks PySpark job task """

    python_path = luigi.Parameter()
    python_params = luigi.ListParameter(default=[])

    def run(self):
        """ Run a new Databricks PySpark job task """

        new_rsub = self.create_runsubmit()

        new_rsub['spark_python_task'] = {
            'python_path': self.python_path,
            'parameters': self.python_params
        }

        self.runssubmit(new_rsub)

        self.wait_for_job()

        self.signal_complete()


class DatabricksSparkJarTask(_DatabricksJobBaseTask):
    """ Databricks Spark jar job task """

    main_class_name = luigi.Parameter()
    jar_params = luigi.ListParameter(default=[])

    def run(self):
        """ Run a new Databricks PySpark job task """

        new_rsub = self.create_runsubmit()

        new_rsub['spark_jar_task'] = {
            'main_class_name': self.main_class_name,
            'parameters': self.jar_params
        }

        self.runssubmit(new_rsub)

        self.wait_for_job()

        self.signal_complete()


class DatabricksSparkSubmitTask(_DatabricksJobBaseTask):
    """ Databricks Spark Submit job task """

    submit_params = luigi.ListParameter(default=[])

    def run(self):
        """ Run a new Databricks PySpark job task """

        new_rsub = self.create_runsubmit()

        new_rsub['spark_submit_task'] = {
            'parameters': self.submit_params
        }

        self.runssubmit(new_rsub)

        self.wait_for_job()

        self.signal_complete()


class _DatabricksClusterBaseTask(_DatabricksBaseTask):
    """ Base task for Databricks clusters """

    def _get_cluster(self, cluster):
        """ check status of cluster until ready """

        req = self.db_request(
            method='get',
            uri='clusters/get',
            params={
                'cluster_id': cluster['cluster_id']
            }
        )

        return req.json()


class CreateDatabricksClusterTask(_DatabricksClusterBaseTask):
    """
    Task for creating a new Databricks cluster
    """

    def run(self):
        """ Run cluster creation """

        cluster = self._cluster_conf()

        LOGGER.info('Creating new Databricks cluster: %s',
                    self.cluster_name)

        req = self.db_request(
            method='post',
            uri='clusters/create',
            json=cluster
        )

        while True:
            time.sleep(10)
            cluster_status = self._get_cluster(req.json())
            state = cluster_status['state']
            cluster_id = req.json()['cluster_id']
            if state == 'RUNNING':
                LOGGER.info('Cluster %s is RUNNING', cluster_id)
                self.signal_complete(cluster_status)
                break
            elif state in ['TERMINATING', 'TERMINATED', 'ERROR', 'UNKNOWN']:
                raise Exception('Cluster %s is in state %s: %s',
                                cluster_id,
                                state,
                                cluster_status['state_message'])


class DeleteDatabricksClusterTask(_DatabricksClusterBaseTask):
    """ Task for deleting an existing Databricks cluster """

    cluster_id = luigi.Parameter(default='')

    def requires(self):
        """
        Must specify the CreateDatabricksClusterTask in addition to any
        dependent job tasks. This is because Databricks cluster names are not unique,
        and cluster_id is unknown until creation time.

        Example::

            return {
                'ClusterCreate': CreateDatabricksClusterTask(...),
                'SomeSparkJob': DatabricksPySparkTask(...)
            }
        """
        pass

    def _get_cluster_id(self):

        if self.cluster_id != '':
            return self.cluster_id

        with self.input()['ClusterCreate'].open('r') as fopen:
            cluster = load(fopen)

        return cluster['cluster_id']

    def run(self):
        """ Ensure deletion of Databricks cluster """

        cluster = {
            'cluster_id': self._get_cluster_id()
        }

        self.db_request(
            method='post',
            uri='/clusters/delete',
            json=cluster
        )

        self.signal_complete()
