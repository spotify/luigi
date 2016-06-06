# -*- coding: utf-8 -*-
#
# Copyright 2016 Thibault JAMET
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
Docker build library.

Allow having docker image build tasks within luigi.
This contrib uses the official python docker client to be installed
with `pip install docker-py`

:class:`DockerClient` is meant to provide a docker client
to sub classes through the `client` attribute

:class:`DockerImageTarget` is meant to provide a target ensuring
that <image_name> is available on the docker_host

:class:`DockerImageBuildTask` is meant to provide a task being responsible
for building a docker image

:class:`DockerTask` is meant to run a given command within a docker container


.. _docker-cli: https://docs.docker.com/engine/reference/commandline/cli/
.. _docker-py: http://docker-py.readthedocs.io/en/stable/api/
.. _docker-py-create-container: http://docker-py.readthedocs.io/en/stable/api/#create_container
"""

from __future__ import absolute_import

import logging
import luigi
import luigi.target
import re
import sys

logger = logging.getLogger('luigi-interface')

try:
    import docker
    import docker.errors
    import docker.utils
except ImportError:
    logger.warning('docker-py is not installed. Please run `pip install docker-py` to run Docker tasks')


class DockerClient(object):
    """
    A base class to provide shared docker client definition
    """

    @property
    def client(self):
        """
        An instance of docker client matching docker-py_ interface

        """
        params = docker.utils.kwargs_from_env(assert_hostname=False)
        host = self.docker_host

        if host:
            params['base_url'] = host
        try:
            self._client
        except AttributeError:
            self._client = docker.Client(**params)
        return self._client


class DockerImageTarget(DockerClient, luigi.target.Target):
    """
    Target for a docker image
    """

    def __init__(self, name, tag='latest', registry=None, docker_host=None):
        """
        :param name: (str) Base name of the image to be built
        :param tag: (str) Tag applied to the image (defaults to 0)
        :param registry: (str) Registry where the image is hosted (defaults to None)
        :param docker_host: (str) Address of the docker socket
            Note: neither TLS nor version selection are supported yet
            See docker-cli_
            and docker-py_
            for more information

        """
        self.docker_host = docker_host
        self.name = name
        self.tag = tag
        self.registry = registry

    @property
    def image_name(self):
        if self.registry:
            return "%s/%s:%s" % (self.registry, self.name, self.tag)
        else:
            return "%s:%s" % (self.name, self.tag)

    def exists(self):
        """
        Checks the existence of the image on the docker host
        """
        try:
            self.client.inspect_image(self.image_name)
            return True
        except docker.errors.NotFound:
            return False


class DockerImageBuildTask(DockerClient, luigi.Task):
    """
    Task building a docker image

    :param name:
        The name of the built image without
        registry and tag.

    :param tag:
        The image tag to be built.

    :param registry:
        The registry on which the image should
        be tagged.

    :param dockerfile:
        Path of the Dockerfile within the build context.
        It is the `Dockerfile.mine` of
        `docker build -t <...> -f Dockerfile.mine .`

    :param path:
        Path of the build context.
        It is the `.` of `docker build -t <...> .`

    :param docker_host: the address of the docker socket
        defaults to None to use docker defaults
        example::

            tcp://localhost:2375
    """
    name = luigi.Parameter()
    tag = luigi.Parameter(default='latest')
    registry = luigi.Parameter(default='')
    dockerfile = luigi.Parameter(default='Dockerfile')
    path = luigi.Parameter(default='.')
    docker_host = luigi.Parameter(default=None)

    def output(self):
        return DockerImageTarget(self.name, tag=self.tag, registry=self.registry)

    def run(self):
        kwds = docker.utils.kwargs_from_env(assert_hostname=False)
        client = docker.Client(**kwds)
        client.build(
            path=self.path,
            tag=self.output().image_name,
            dockerfile=self.dockerfile
        )


class DockerTask(DockerClient, luigi.Task):

    """
    Base class for a Docker container task. Like `docker run` command,
    Image will be pulled when not available unless explicitely
    instructed to pull it in any case.

    :param image: The name of the image to run, include registry and tag
        Example::

            docker.company.com/namespace/image:tag

    :param pull: Pulls the image even if it already exists on the host

    :param remove: Instructs image removal at the end of the run

    :param remove_volumes: Inctructs to remove volumes when removing image.
        It has the same effects of the `-v` flag in `docker rm -v <container>`

    :param create_args: a dict of arguments provided to docker create.
        see docker-py-create-container_
        for a full description example::

            create_args = {
                'image': 'hello-docker',
            }

    :param docker_host: the address of the docker socket
        defaults to None to use docker defaults
        example::

            tcp://localhost:2375
    """

    imageNameRe = re.compile(r'^([^:]*)(?::([^:]*)|)$')
    image = luigi.Parameter()
    pull = luigi.BoolParameter(default=False, significant=False)
    remove = luigi.BoolParameter(default=False, significant=False)
    remove_volumes = luigi.BoolParameter(default=True, significant=False)
    create_args = luigi.DictParameter(default=None)
    docker_host = luigi.Parameter(default=None)

    @property
    def command(self):
        """
        Command passed to the containers

        Override to return list of dicts with keys 'name' and 'command',
        describing the container names and commands to pass to the container.

        *Note*: The current API only supports a single command. List is provided
        for a coherent interface with ECSTask

        For example::

            [
                {
                    'name': 'myContainer',
                    'command': ['/bin/sleep', '60']
                }
            ]
        """
        pass

    def run(self):
        """
        The actual run part.

        Ran image resolution order is:

        1. self.command[0]['image']
        2. create_args['image']
        3. task.image

        Container name resolution order is:

        1. self.command[0]['name']
        2. create_args['name']
        3. the default docker name allocation

        Container command resolution order is:

        1. self.command[0]['command']
        2. create_args['command']
        3. command specified in image
        """
        command = self.command
        # duplicate dict handle to protect original
        # one. We will update it
        if self.create_args:
            create_args = dict(self.create_args)
        else:
            create_args = {}

        if self.image:
            create_args['image'] = self.image

        if command:
            if len(command) != 1:
                raise ValueError(('Current implementation of DockerTask only'
                                  'supports a single command'))
            create_args.update(command[0])

        # Either when the user requests a systematic pull
        # or when the image does not exist on the docker host,
        # perform a `docker pull`
        pull = False
        if self.pull:
            pull = True
        else:
            try:
                self.client.inspect_image(create_args['image'])
            except docker.errors.NotFound:
                logger.debug("%s does not exist locally, pull it" %
                             create_args['image'])
                pull = True
        if pull:
            name, tag = self.imageNameRe.match(
                create_args['image']
            ).groups()
            if tag is None:
                tag = 'latest'
            # silently pull the image
            self.client.pull(name, tag)

        container = self.client.create_container(**create_args)
        self.client.start(container.get('Id'))
        for line in self.client.logs(
            container.get('Id'), stream=True,
            stdout=True, stderr=True
        ):
            sys.stdout.write(line)
            sys.stdout.flush()

        exit_code = self.client.inspect_container(container).get("State", {}).get("ExitCode")

        if self.remove:
            self.client.remove_container(
                container.get('Id'),
                v=(
                    True if self.remove_volumes else False
                )
            )

        if exit_code != 0:
            raise RuntimeError("Failed to run command %r in image %s" % (create_args.get('command', None), create_args.get('image', None)))
