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
- Amazon AWS credentials discoverable by boto3 (e.g., by using
  ``aws configure`` from awscli_)
- A running ECS cluster (see `ECS Get Started`_)

Written and maintained by Jake Feala (@jfeala) for Outlier Bio (@outlierbio)

.. _`docker run`: https://docs.docker.com/reference/commandline/run
.. _taskDefinition:
    http://docs.aws.amazon.com/AmazonECS/latest/developerguide/task_defintions.html
.. _`boto3-powered`: https://boto3.readthedocs.io
.. _awscli: https://aws.amazon.com/cli
.. _`ECS Get Started`:
    http://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_GetStarted.html

"""

import time
import logging
import luigi

logger = logging.getLogger('luigi-interface')

try:
    import boto3
    client = boto3.client('ecs')
except ImportError:
    logger.warning('boto3 is not installed. ECSTasks require boto3')

POLL_TIME = 2


def _get_task_descriptions(cluster, task_ids):
    """
    Retrieve task descriptions from ECS API

    Returns list of {RUNNING|PENDING|STOPPED} for each id in task_ids
    """
    response = client.describe_tasks(cluster=cluster, tasks=task_ids)

    # Error checking
    if response['failures'] != []:
        raise Exception('There were some failures:\n{0}'.format(
            response['failures']))

    status_code = response['ResponseMetadata']['HTTPStatusCode']
    if status_code != 200:
        msg = 'Task status request received status code {0}:\n{1}'
        raise Exception(msg.format(status_code, response))

    return response['tasks']

def _get_task_statuses(tasks):
    """
    Returns list of {RUNNING|PENDING|STOPPED} for each task in tasks

    Args:
        - tasks (list): list of ECS taskDescriptions
    """
    return [t['lastStatus'] for t in tasks]

def _check_exit_codes(tasks, essential_containers):
    """
    Checks each essential task in tasks for a failure reason or a
    non-zero exitCode

    Args:
        - tasks (list): list of ECS taskDescriptions
        - essential_containers (list): list of essential container names

    Raises:
        - Exception: A failing essential task was found
    """
    dirty_exits = []
    for t in tasks:
        for container in t['containers']:
            if container['name'] in essential_containers:
                if container['lastStatus'] == 'STOPPED':
                    # Check if container's command returned error
                    # or if ECS had an error running the command
                    if 'exitCode' in container and container['exitCode'] != 0 \
                            or 'reason' in container:
                        dirty_exits.append(container)

    if len(dirty_exits):
        raise Exception('Some containers had non-zero exit codes:\n{0}'.format(
            dirty_exits))

def _track_tasks(cluster, task_ids, essential_containers):
    """Poll task status until STOPPED"""
    while True:
        task_descriptions = _get_task_descriptions(cluster, task_ids)
        statuses = _get_task_statuses(task_descriptions)
        if all([status == 'STOPPED' for status in statuses]):
            _check_exit_codes(task_descriptions, essential_containers)
            logger.info('ECS tasks {0} STOPPED'.format(','.join(task_ids)))
            break
        time.sleep(POLL_TIME)
        logger.debug('ECS task status for tasks {0}: {1}'.format(
            ','.join(task_ids), statuses))


class ECSTask(luigi.Task):

    """
    Base class for an Amazon EC2 Container Service Task

    Amazon ECS requires you to register "tasks", which are JSON descriptions
    for how to issue the ``docker run`` command. This Luigi Task can either
    run a pre-registered ECS taskDefinition, OR register the task on the fly
    from a Python dict.

    :param task_def_arn: pre-registered task definition ARN (Amazon Resource
        Name), of the form::

            arn:aws:ecs:<region>:<user_id>:task-definition/<family>:<tag>

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

    task_def_arn = luigi.Parameter(default=None)
    task_def = luigi.Parameter(default=None)
    cluster = luigi.Parameter(default='default')

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
        if (not self.task_def and not self.task_def_arn) or \
           (self.task_def and self.task_def_arn):
            raise ValueError(('Either (but not both) a task_def (dict) or'
                              'task_def_arn (string) must be assigned'))
        if not self.task_def_arn:
            # Register the task and get assigned taskDefinition ID (arn)
            response = client.register_task_definition(**self.task_def)
            self.task_def_arn = response['taskDefinition']['taskDefinitionArn']

        # Submit the task to AWS ECS and get assigned task ID
        # (list containing 1 string)
        if self.command:
            overrides = {'containerOverrides': self.command}
        else:
            overrides = {}
        response = client.run_task(
            cluster=self.cluster,
            taskDefinition=self.task_def_arn,
            overrides=overrides)

        self._task_ids = [task['taskArn'] for task in response['tasks']]

        # Get essential container names to fail early on errors
        task_definition = client.describe_task_definition(
            taskDefinition=self.task_def_arn)
        task_definition = task_definition['taskDefinition']
        self._essential_containers = \
            [cont['name']
                for cont in task_definition['containerDefinitions']
                if cont['essential']]

        # Wait on task completion
        _track_tasks(self.cluster, self._task_ids, self._essential_containers)
