# -*- coding: utf-8 -*-
#
# Copyright 2017 Open Targets
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
Docker container wrapper for Luigi.

Enables using any docker container as a task in luigi

Requires:

- docker: ``pip install docker``

Written and maintained by Andrea Pierleoni (@apierleoni).
"""
import json
import tempfile
from tempfile import mkdtemp

import luigi
import logging

from luigi.local_target import LocalFileSystem

logger = logging.getLogger('luigi-interface')

try:
    import docker
    from docker.errors import ContainerError, ImageNotFound, APIError

except ImportError:
    logger.warning('docker is not installed. DockerTask requires docker.')
    docker = None


class DockerTask(luigi.Task):

    @property
    def image(self):
        return "alpine"

    @property
    def command(self):
        return "echo hello world"

    @property
    def name(self):
        return ''

    @property
    def container_options(self):
        return {}

    @property
    def volumes(self):
        return []

    @property
    def network_mode(self):
        return ''

    @property
    def docker_url(self):
        return None

    @property
    def auto_remove(self):
        return True

    @property
    def environment(self):
        return {}

    @property
    def tmp_dir(self):
        return '/tmp/luigi'

    @property
    def force_pull(self):
        return False


    def run(self):
        self.__logger = logger

        '''init docker client
        using the low level API as the higher level API does not allow to mount single
        files as volumes
        '''
        client = docker.APIClient(self.docker_url)


        '''create temp dir'''
        tempfile.tempdir = '/tmp' #set it explicitely to make it work out of the box in mac os
        host_tmp_dir =  mkdtemp(suffix=self.name, prefix='luigi-docker-tmp-dir-',)
        environment = self.environment
        environment['LUIGI_TMP_DIR'] = self.tmp_dir
        volumes = self.volumes
        mounted_volumes = []
        if isinstance(volumes, list):
            volumes.append('{0}:{1}'.format(host_tmp_dir, self.tmp_dir))
            mounted_volumes = [v.split(':')[1] for v in volumes ]

        '''get image if missing'''
        if self.force_pull or len(client.images(name=self.image)) == 0:
            logger.info('Pulling docker image ' + self.image)
            for l in client.pull(self.image, stream=True):
                logger.debug(l.decode('utf-8'))

        '''remove clashing container if needed'''
        if self.auto_remove and self.name:
            try:
                client.remove_container(self.name,
                                        force=True)
            except APIError as e:
                self.__logger.warning("Ignored error in Docker API: " + e.explanation)


        '''run the container'''
        try:
            container=client.create_container(self.image,
                                  command=self.command,
                                  name=self.name,
                                  environment=environment,
                                  volumes = mounted_volumes,
                                  host_config=client.create_host_config(binds=volumes,
                                                                        network_mode=self.network_mode),
                                  **self.container_options)
            client.start(container['Id'])

            exit_status = client.wait(container['Id'])
            if exit_status != 0:
                stdout = False
                stderr = True
                error = client.logs(container['Id'],stdout=stdout, stderr=stderr)
            if self.auto_remove:
                client.remove_container(container['Id'])
            if exit_status != 0:
                raise ContainerError(container, exit_status, self.command, self.image, error)


        except ContainerError as e:
            '''catch non zero exti status and return it'''
            container_name = ''
            if self.name:
                container_name = self.name
            try:
                message = e.message
            except:
                message = str(e)
            self.__logger.error("Container" + container_name + " exited with non zero code: "+ message)
            raise
        except ImageNotFound as e:
            self.__logger.error("Image" + self.image + " not found")
            raise
        except APIError as e:
            self.__logger.error("Error in Docker API: "+e.explanation )
            raise

        '''delete temp dir'''
        fs = LocalFileSystem()
        if fs.exists(host_tmp_dir):
            fs.remove(host_tmp_dir, recursive=True)






