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

import luigi
import logging


logger = logging.getLogger('luigi-interface')

try:
    import docker
    from docker.errors import ContainerError, ImageNotFound, APIError

except ImportError:
    logger.warning('docker is not installed. DockerTask requires docker.')


class DockerTask(luigi.Task):

    @property
    def image(self):
        return "alpine"

    @property
    def command(self):
        return "echo hello world"

    @property
    def name(self):
        return None

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
    def docker_api_addr(self):
        return None

    @property
    def auto_remove(self):
        return True


    def run(self):
        self.__logger = logger
        client = docker.APIClient(self.docker_api_addr)
        try:
            container=client.create_container(self.image,
                                  command=self.command,
                                  name = self.name,
                                  host_config=client.create_host_config(binds=self.volumes,
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
            self.__logger.error("Container" + container_name + " exited with non zero code: "+ e.message)
            raise
        except ImageNotFound as e:
            self.__logger.error("Image" + self.image + " not found")
            raise
        except APIError as e:
            self.__logger.error("Error in Docker API: "+e.explanation )
            raise
        print 'hi'





