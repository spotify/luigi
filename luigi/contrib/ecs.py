# -*- coding: utf-8 -*-
#
# Copyright 2015 Outlier Bio, LLC
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
EC2 Container Service wrapper for Luigi

From the AWS website:

  Amazon EC2 Container Service (ECS) is a highly scalable, high performance
  container management service that supports Docker containers and allows you
  to easily run applications on a managed cluster of Amazon EC2 instances.

To use ECS, you create a taskDefinition_ JSON that defines the `docker run`_
command for one or more containers in a task or service, and then submit this
JSON to the API to run the task.

This `boto3-powered`_ wrapper allows you to create Luigi Tasks to submit ECS
``taskDefinition`` s. You can either pass a dict (mapping directly to the
``taskDefinition`` JSON) OR an Amazon Resource Name (arn) for a previously
registered ``taskDefinition``.

Requires:

- boto3 package
- Amazon AWS credentials discoverable by boto3 (e.g., by using ``aws configure``
  from awscli_)
- A running ECS cluster (see `ECS Get Started`_)

Written and maintained by Jake Feala (@jfeala) for Outlier Bio (@outlierbio)

.. _`docker run`: https://docs.docker.com/reference/commandline/run
.. _taskDefinition: http://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_defintions.html
.. _`boto3-powered`: https://boto3.readthedocs.io
.. _awscli: https://aws.amazon.com/cli
.. _`ECS Get Started`: http://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_GetStarted.html

"""

from botocore.exceptions import WaiterError
import boto3
import logging
import luigi

logger = logging.getLogger('luigi-interface')
client = boto3.client('ecs')


def _track_tasks(cluster, task_ids, max_attempts, track_delay):
    """Wait task until is STOPPED"""

    for state in ['running', 'stopped']:
        waiter = client.get_waiter('tasks_{0}'.format(state))

        if max_attempts and state in max_attempts:
            waiter.config.max_attempts = max_attempts[state]
        if track_delay and state in track_delay:
            waiter.config.delay = track_delay[state]

        try:
            logger.debug('Waiting for ECS tasks {0} to {1} on {2}'.format(','.join(task_ids), state, cluster))
            waiter.wait(cluster=cluster, tasks=task_ids)
        except WaiterError as e:
            reason = str(e)
            logger.info(reason)
            if state == 'running' and 'Max attempts exceeded' in reason:
                for task_id in task_ids:
                    client.stop_task(cluster=cluster, task=task_ids, reason=reason)
            raise e

        logger.info('ECS tasks {0} {1}'.format(','.join(task_ids), state))


class ECSTask(luigi.Task):

    """
    Base class for an Amazon EC2 Container Service Task

    Amazon ECS requires you to register "tasks", which are JSON descriptions
    for how to issue the ``docker run`` command. This Luigi Task can either
    run a pre-registered ECS taskDefinition, OR register the task on the fly
    from a Python dict.

    :param task_family: pre-registered task definition family of the form::

            <family>:<tag>

    :param task_def: dict describing task in taskDefinition JSON format, for
        example::

            task_def = {
                'family': 'hello-world',
                'volumes': [],
                'containerDefinitions': [
                    {
                        'memory': 1,
                        'essential': True,
                        'name': 'hello-world',
                        'image': 'ubuntu',
                        'command': ['/bin/echo', 'hello world']
                    }
                ]
            }

    """

    task_def = luigi.Parameter(default=None)
    cluster = luigi.Parameter(default=None)
    family = luigi.Parameter(default=None)

    max_attempts = luigi.DictParameter(default={})
    track_delay = luigi.DictParameter(default={})

    @property
    def ecs_task_ids(self):
        """Expose the ECS task ID"""
        if hasattr(self, '_task_ids'):
            return self._task_ids

    @property
    def command(self):
        """
        Command passed to the containers
        Override to return list of dicts with keys 'name' and 'command',
        describing the container names and commands to pass to the container.
        Directly corresponds to the `overrides` parameter of runTask API. For
        example::
            [
                {
                    'name': 'myContainer',
                    'command': ['/bin/sleep', '60']
                }
            ]
        """
        pass

    def run(self):
        if (not self.task_def and not self.family) or \
           (self.task_def and self.family):
            raise ValueError(('Either (but not both) a task_def (dict) or'
                              'task_def_arn (string) must be assigned'))
        if not self.family:
            # Register the task and get assigned taskDefinition ID (arn)
            response = client.register_task_definition(**self.task_def)
            self.family = response['taskDefinition']['taskDefinitionArn']
            logger.info('ECS tasks registered')

        # Submit the task to AWS ECS and get assigned task ID
        # (list containing 1 string)
        if self.command:
            overrides = {'containerOverrides': self.command}
        else:
            overrides = {}

        response = client.run_task(cluster=self.cluster,
                                   taskDefinition=self.family,
                                   overrides=overrides)
        if response['failures']:
            raise Exception(", ".join(["fail to run task {0} reason: {1}".format(failure['arn'], failure['reason'])
                                       for failure in response['failures']]))

        self._task_ids = [task['taskArn'] for task in response['tasks']]
        if all([task['lastStatus'] == 'PENDING' for task in response['tasks']]):
            logger.info('ECS tasks {0} {1}'.format(','.join(self._task_ids), 'pending'))

        # Wait on task completion
        _track_tasks(self.cluster, self._task_ids, self.max_attempts, self.track_delay)

if __name__ == '__main__':
    luigi.run()
