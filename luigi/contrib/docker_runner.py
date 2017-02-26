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
        return "luigi-test"

    @property
    def container_options(self):
        return {}


    def run(self):
        self.__logger = logger
        client = docker.from_env()
        try:
            client.containers.run(self.image,
                                  command=self.command,
                                  name = self.name,
                                  stdout=True,
                                  stderr=False,
                                  remove=True,
                                  **self.container_options)
        except ContainerError as e:
            self.__logger.error("Container" + self.name + " exited with non zero code: "+ e.message)
            raise
        except ImageNotFound as e:
            self.__logger.error("Image" + self.image + " not found")
            raise
        except APIError as e:
            self.__logger.error("Error in Docker API: "+e.explanation )
            raise




