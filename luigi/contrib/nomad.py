#
# Copyright 2021 Volvo Car Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""
Nomad Job wrapper for Luigi.

From the Nomad website:

    A simple and flexible workload orchestrator to deploy and manage containers
    and non-containerized applications across on-prem and clouds at scale.

For more information about Nomad: https://www.nomadproject.io/

Requires:

- nomad: ``pip install python-nomad``

Written by Anders Björklund
"""
import logging
import subprocess
import time
import uuid
from datetime import datetime

import luigi

logger = logging.getLogger('luigi-interface')

try:
    from nomad import Nomad
except ImportError:
    logger.warning('nomad is not installed. NomadJobTask requires nomad.')


class nomad(luigi.Config):
    nomad_secure = luigi.BoolParameter(
        default=False,
        description="Nomad secure")
    nomad_host = luigi.Parameter(
        default="localhost",
        description="Nomad host")
    nomad_port = luigi.IntParameter(
        default=4646,
        description="Nomad port")
    max_retrials = luigi.IntParameter(
        default=0,
        description="Max retrials in event of job failure")
    nomad_namespace = luigi.OptionalParameter(
        default=None,
        description="Nomad namespace in which the job will run")


class NomadJobTask(luigi.Task):
    __DEFAULT_POLL_INTERVAL = 5  # see __track_job
    __DEFAULT_ALLOCATION_INTERVAL = 5
    _nomad_config = None  # Needs to be loaded at runtime

    def _init_nomad(self):
        self.__logger = logger
        self.__logger.debug("Nomad host: " + self.nomad_host)
        self.__nomad = Nomad(secure=self.nomad_secure, host=self.nomad_host, port=self.nomad_port,
                             region=None, namespace=self.nomad_namespace, token=None)
        self.job_uuid = str(uuid.uuid4().hex)
        now = datetime.utcnow()
        self.uu_name = "%s-%s-%s" % (self.name, now.strftime('%Y%m%d%H%M%S'), self.job_uuid[:16])

    @property
    def nomad_secure(self):
        """
        SSL for Nomad.
        """
        return self.nomad_config.nomad_secure

    @property
    def nomad_host(self):
        """
        Host for Nomad.
        """
        return self.nomad_config.nomad_host

    @property
    def nomad_port(self):
        """
        Port for Nomad.
        """
        return self.nomad_config.nomad_port

    def nomad_address(self):
        if self.nomad_secure:
            scheme = "https"
        else:
            scheme = "http"
        return "%s://%s:%d" % (scheme, self.nomad_host, self.nomad_port)

    @property
    def nomad_namespace(self):
        """
        Namespace in Nomad where the job will run.
        It defaults to the default namespace in Nomad
        """
        return self.nomad_config.nomad_namespace

    @property
    def name(self):
        """
        A name for this job. This task will automatically append a UUID to the
        name before to submit to Nomad.
        """
        raise NotImplementedError("subclass must define name")

    @property
    def labels(self):
        """
        Return custom labels for nomad job.
        example::
        ``{"run_dt": datetime.date.today().strftime('%F')}``
        """
        return {}

    @property
    def spec_schema(self):
        """
        Nomad Job spec schema in JSON format, an example follows.

        .. code-block:: javascript

            {
                "Type": "batch",
                "TaskGroups": [{
                    "Name": "main",
                    "Tasks": [{
                        "Driver": "docker",
                        "Name": "pi",
                        "Config": {
                            "image": "perl",
                            "command": "perl",
                            "args": ["-Mbignum=bpi", "-wle", "print bpi(2000)"]
                        },
                    }]
                }]
            }

        **Schedulers**

        - If Type is not defined, it will be set to "batch" by default.

        For more informations please refer to:
        https://www.nomadproject.io/docs/schedulers
        """
        raise NotImplementedError("subclass must define spec_schema")

    @property
    def max_retrials(self):
        """
        Maximum number of retrials in case of failure.
        """
        return self.nomad_config.max_retrials

    @property
    def delete_on_success(self):
        """
        Deregister the Nomad job if it has completed successfully.
        """
        return True

    @property
    def print_allocation_logs_on_exit(self):
        """
        Fetch and print the allocation logs once the job is completed.
        """
        return False

    @property
    def nomad_config(self):
        if not self._nomad_config:
            self._nomad_config = nomad()
        return self._nomad_config

    @property
    def poll_interval(self):
        """How often to poll Nomad for job status, in seconds."""
        return self.__DEFAULT_POLL_INTERVAL

    @property
    def allocation_wait_interal(self):
        """Delay for initial allocation for just submitted job in seconds"""
        return self.__DEFAULT_ALLOCATION_INTERVAL

    def __track_job(self):
        """Poll job status while active"""
        while not self.__verify_job_has_started():
            time.sleep(self.poll_interval)
            self.__logger.debug("Waiting for Nomad job " + self.uu_name + " to start")
        self.__print_nomad_hints()

        status = self.__get_job_status()
        while status == "RUNNING":
            self.__logger.debug("Nomad job " + self.uu_name + " is running")
            time.sleep(self.poll_interval)
            status = self.__get_job_status()

        assert status != "FAILED", "Nomad job " + self.uu_name + " failed"

        # status == "SUCCEEDED"
        self.__logger.info("Nomad job " + self.uu_name + " succeeded")
        self.signal_complete()

    def signal_complete(self):
        """Signal job completion for scheduler and dependent tasks.

         Touching a system file is an easy way to signal completion. example::
         .. code-block:: python

         with self.output().open('w') as output_file:
             output_file.write('')
        """
        pass

    def __get_allocations(self):
        return self.__nomad.job.get_allocations(self.uu_name)

    def __get_job(self):
        assert self.uu_name in self.__nomad.jobs, "Nomad job " + self.uu_name + " not found"
        return self.__nomad.job.get_job(self.uu_name)

    def __print_allocation_logs(self):
        for allocation in self.__get_allocations():
            # currently logs seem to be missing from python-nomad's API, use nomad CLI
            logs = subprocess.check_output(["nomad", "alloc", "logs",
                                            "-address=%s" % self.nomad_address(),
                                            allocation["ID"]], universal_newlines=True).strip()
            self.__logger.info("Fetching logs from " + allocation["Name"])
            if len(logs) > 0:
                for line in logs.split('\n'):
                    self.__logger.info(line)

    def __print_nomad_hints(self):
        self.__logger.info("To stream allocation logs, use:")
        self.__logger.info("`nomad logs -f -job %s`" % (self.uu_name))

    def __verify_job_has_started(self):
        """Asserts that the job has successfully started"""
        # Verify that the job started
        job = self.__get_job()

        # Verify that allocation exists
        allocations = self.__get_allocations()
        if not allocations:
            self.__logger.debug(
                'No allocations found for %s, waiting for cluster state to match the job definition' % self.uu_name
            )
            time.sleep(self.allocation_wait_interal)
            allocations = self.__get_allocations()

        assert len(allocations) > 0, "No allocations scheduled by " + self.uu_name
        if job["Status"] == "pending":
            return False

        return True

    def __get_job_status(self):
        """Return the Nomad job status"""
        # Figure out status and return it
        job = self.__get_job()

        if job["Status"] == "dead":
            allocations = self.__nomad.job.get_allocations(self.uu_name)
            for allocation in allocations:
                if allocation["ClientStatus"] == "complete":
                    if self.print_allocation_logs_on_exit:
                        self.__print_allocation_logs()
                    if self.delete_on_success:
                        self.__nomad.job.deregister_job(self.uu_name)
                    return "SUCCEEDED"
                elif allocation["ClientStatus"] == "failed":
                    self.__logger.debug("Nomad job " + self.uu_name + " failed")
                    if self.print_allocation_logs_on_exit:
                        self.__print_allocation_logs()
                    return "FAILED"
        return "RUNNING"

    def run(self):
        self._init_nomad()
        # Render job
        job_json = {"Job": self.spec_schema}
        job_json["Job"]["Id"] = self.uu_name
        if "Type" not in job_json["Job"]:
            job_json["Job"]["Type"] = "batch"
        if "Datacenters" not in job_json["Job"]:
            job_json["Job"]["Datacenters"] = ["dc1"]
        labels = {
            "spawned_by": "luigi",
            "luigi_task_id": self.job_uuid
        }
        if job_json['Job'].get('Meta') is None:
            job_json['Job']['Meta'] = labels
        else:
            job_json['Job']['Meta'].update(labels)
        if self.nomad_namespace is not None:
            job_json['Namespace'] = self.nomad_namespace
        # Update user labels
        job_json['Job']['Meta'].update(self.labels)

        # Add default Policy if not specified
        if "TaskGroups" in job_json["Job"]:
            for tg in job_json["Job"]["TaskGroups"]:
                if "RestartPolicy" not in tg:
                    tg["RestartPolicy"] = {
                        "Attempts": self.max_retrials
                    }
                if "ReschedulePolicy" not in tg:
                    tg["ReschedulePolicy"] = {
                        "Attempts": 0
                    }
        # Submit job
        self.__logger.info("Submitting Nomad Job: " + self.uu_name)
        self.__nomad.job.register_job(self.uu_name, job_json)

        # Track the Job (wait while active)
        self.__logger.info("Start tracking Nomad Job: " + self.uu_name)
        self.__track_job()

    def output(self):
        """
        An output target is necessary for checking job completion unless
        an alternative complete method is defined.

        Example::

            return luigi.LocalTarget(os.path.join('/tmp', 'example'))

        """
        pass
