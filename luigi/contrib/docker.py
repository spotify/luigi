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


.. _docker-cli: https://docs.docker.com/engine/reference/commandline/cli/
.. _docker-py: http://docker-py.readthedocs.io/en/stable/api/
.. _docker-py-create-container: http://docker-py.readthedocs.io/en/stable/api/#create_container
"""

from __future__ import absolute_import

import logging
import luigi
import luigi.target

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
