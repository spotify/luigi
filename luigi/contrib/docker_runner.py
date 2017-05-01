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
from tempfile import mkdtemp
import uuid
import logging
import luigi

from luigi.local_target import LocalFileSystem
from luigi import six

logger = logging.getLogger('luigi-interface')

try:
    import docker
    from docker.errors import ContainerError, ImageNotFound, APIError

except ImportError:
    logger.warning('docker is not installed. DockerTask requires docker.')
    docker = None

# TODO: may need to implement this logic for remote hosts
# class dockerconfig(luigi.Config):
#     '''
#     this class allows to use the luigi.cfg file to specify the path to the docker config.json. The docker client should look by default in the main directory, but on different systems this may need to be specified. 
#     '''
#     docker_config_path = luigi.Parameter(
#         default="~/.docker/config.json",
#         description="Path to dockercfg file for authentication")


class DockerTask(luigi.Task):

    @property
    def image(self):
        return 'alpine'

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
    def environment(self):
        return {}

    @property
    def container_tmp_dir(self):
        return '/tmp/luigi'

    @property
    def volumes(self):
        '''
        Override this to mount local volumes, in addition to the /tmp/luigi
        which gets defined by default. This should return a list of strings.
        e.g. ['/hostpath1:/containerpath1', '/hostpath2:/containerpath2']
        '''
        return None


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
    def force_pull(self):
        return False

    def __init__(self):
        '''
        When a new instance of the DockerTask class gets created:
        - call the parent class __init__ method
        - start the logger
        - init an instance of the docker client
        - create a tmp dir
        - add the temp dir to the volume binds specified in the task
        '''
        super(DockerTask, self).__init__()
        self.__logger = logger
        
        '''init docker client
        using the low level API as the higher level API does not allow to mount single
        files as volumes
        '''
        self._client = docker.APIClient(self.docker_url)

        # add latest tag if nothing else is specified by task
        if ':' not in self.image:
            self._image = ':'.join([self.image,'latest'])
        else:
            self._image = self.image

        # create a tmp_dir, NOTE: /tmp needs to be specified for it to work on
        # macOS, despite what the python documentation says
        self._host_tmp_dir = mkdtemp(suffix=self.task_id,
                                     prefix='luigi-docker-tmp-dir-',
                                     dir='/tmp')

        self._volumes = ['{0}:{1}'.format(self._host_tmp_dir, self.container_tmp_dir)]
        # update environment property with the (internal) location of tmp_dir
        self.environment['LUIGI_TMP_DIR'] = self.container_tmp_dir

        logger.debug('>>ELISEO<< self._volumes is {0}'.format(self._volumes))
        logger.debug('>>ELISEO<< self.volume is {0}'.format(self.volumes))

        # add additional volume binds specified by the user to the tmp_Dir bind
        if isinstance(self.volumes, six.string_types):
            return self._volumes.append(self.volumes)
        elif isinstance(self.volumes, list):
            return self._volumes.append(*self.volumes)


    def run(self):

        # get image if missing
        if self.force_pull or len(self._client.images(name=self._image)) == 0:
            logger.info('Pulling docker image ' + self._image)
            try:
                for logline in self._client.pull(self._image, stream=True):
                    logger.debug(logline.decode('utf-8'))
            except APIError as e:
                self.__logger.warning("Error in Docker API: " + e.explanation)
                raise

        # remove clashing container if a container with the same name exists
        if self.auto_remove and self.name:
            try:
                self._client.remove_container(self.name,
                                             force=True)
            except APIError as e:
                self.__logger.warning("Ignored error in Docker API: " + e.explanation)

        # run the container
        try:
            logger.debug('>>ELISEO<< self.volumes is {0}'.format(self.volumes))

            container = self._client.create_container(self._image,
                                                command=self.command,
                                                name=self.name,
                                                environment=self.environment,
                                                # volumes = mounted_volumes,
                                                host_config=self._client.create_host_config(binds=self._volumes,
                                                                                      network_mode=self.network_mode),
                                                **self.container_options)
            self._client.start(container['Id'])

            exit_status = self._client.wait(container['Id'])
            if exit_status != 0:
                stdout = False
                stderr = True
                error = self._client.logs(container['Id'],
                                    stdout=stdout,
                                    stderr=stderr)
            if self.auto_remove:
                self._client.remove_container(container['Id'])
            if exit_status != 0:
                raise ContainerError(container, exit_status, self.command, self._image, error)

        except ContainerError as e:
            # catch non zero exti status and return it
            container_name = ''
            if self.name:
                container_name = self.name
            try:
                message = e.message
            except:
                message = str(e)
            self.__logger.error("Container " + container_name +
                                " exited with non zero code: " + message)
            raise
        except ImageNotFound as e:
            self.__logger.error("Image " + self._image + " not found")
            raise
        except APIError as e:
            self.__logger.error("Error in Docker API: "+e.explanation)
            raise

        # delete temp dir
        filesys = LocalFileSystem()
        if filesys.exists(self._host_tmp_dir):
            filesys.remove(self._host_tmp_dir, recursive=True)
