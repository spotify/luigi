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
Tests for Docker container wrapper for Luigi.


Requires:

- docker: ``pip install docker``

Written and maintained by Andrea Pierleoni (@apierleoni).
Contributions by Eliseo Papa (@elipapa)
"""
import tempfile
from helpers import unittest
from tempfile import NamedTemporaryFile

import luigi
import logging
from luigi.contrib.docker_runner import DockerTask

import pytest

logger = logging.getLogger('luigi-interface')

try:
    import docker
    from docker.errors import ContainerError, ImageNotFound
    client = docker.from_env()
    client.version()
except ImportError:
    raise unittest.SkipTest('Unable to load docker module')
except Exception:
    raise unittest.SkipTest('Unable to connect to docker daemon')

tempfile.tempdir = '/tmp'  # set it explicitely to make it work out of the box in mac os
local_file = NamedTemporaryFile()
local_file.write(b'this is a test file\n')
local_file.flush()


class SuccessJob(DockerTask):
    image = "busybox:latest"
    name = "SuccessJob"


class FailJobImageNotFound(DockerTask):
    image = "image-does-not-exists"
    name = "FailJobImageNotFound"


class FailJobContainer(DockerTask):
    image = "busybox"
    name = "FailJobContainer"
    command = 'cat this-file-does-not-exist'


class WriteToTmpDir(DockerTask):
    image = "busybox"
    name = "WriteToTmpDir"
    container_tmp_dir = '/tmp/luigi-test'
    command = 'test -d  /tmp/luigi-test'
    # command = 'test -d $LUIGI_TMP_DIR'# && echo ok >$LUIGI_TMP_DIR/test'


class MountLocalFileAsVolume(DockerTask):
    image = "busybox"
    name = "MountLocalFileAsVolume"
    # volumes= {'/tmp/local_file_test': {'bind': local_file.name, 'mode': 'rw'}}
    binds = [local_file.name + ':/tmp/local_file_test']
    command = 'test -f /tmp/local_file_test'


class MountLocalFileAsVolumeWithParam(DockerTask):
    dummyopt = luigi.Parameter()
    image = "busybox"
    name = "MountLocalFileAsVolumeWithParam"
    binds = [local_file.name + ':/tmp/local_file_test']
    command = 'test -f /tmp/local_file_test'


class MountLocalFileAsVolumeWithParamRedefProperties(DockerTask):
    dummyopt = luigi.Parameter()
    image = "busybox"
    name = "MountLocalFileAsVolumeWithParamRedef"

    @property
    def binds(self):
        return [local_file.name + ':/tmp/local_file_test' + self.dummyopt]

    @property
    def command(self):
        return 'test -f /tmp/local_file_test' + self.dummyopt

    def complete(self):
        return True


class MultipleDockerTask(luigi.WrapperTask):
    '''because the volumes property is defined as a list, spinning multiple
    containers led to conflict in the volume binds definition, with multiple
    host directories pointing to the same container directory'''
    def requires(self):
        return [MountLocalFileAsVolumeWithParam(dummyopt=opt)
                for opt in ['one', 'two', 'three']]


class MultipleDockerTaskRedefProperties(luigi.WrapperTask):
    def requires(self):
        return [MountLocalFileAsVolumeWithParamRedefProperties(dummyopt=opt)
                for opt in ['one', 'two', 'three']]


@pytest.mark.contrib
class TestDockerTask(unittest.TestCase):

    # def tearDown(self):
    #     local_file.close()

    def test_success_job(self):
        success = SuccessJob()
        luigi.build([success], local_scheduler=True)
        self.assertTrue(success)

    def test_temp_dir_creation(self):
        writedir = WriteToTmpDir()
        writedir.run()

    def test_local_file_mount(self):
        localfile = MountLocalFileAsVolume()
        localfile.run()

    def test_fail_job_image_not_found(self):
        fail = FailJobImageNotFound()
        self.assertRaises(ImageNotFound, fail.run)

    def test_fail_job_container(self):
        fail = FailJobContainer()
        self.assertRaises(ContainerError, fail.run)

    def test_multiple_jobs(self):
        worked = MultipleDockerTask()
        luigi.build([worked], local_scheduler=True)
        self.assertTrue(worked)

    def test_multiple_jobs2(self):
        worked = MultipleDockerTaskRedefProperties()
        luigi.build([worked], local_scheduler=True)
        self.assertTrue(worked)
