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
"""
import tempfile
import unittest
from tempfile import NamedTemporaryFile

import luigi
import logging
from luigi.contrib.docker_runner import DockerTask

logger = logging.getLogger('luigi-interface')

try:
    import docker
    from docker.errors import ContainerError, ImageNotFound
except ImportError:
    raise unittest.SkipTest('docker is not installed. This test requires docker.')

tempfile.tempdir = '/tmp' #set it explicitely to make it work out of the box in mac os
local_file = NamedTemporaryFile().name
print local_file

class SuccessJob(DockerTask):
    image = "alpine"
    name = "SuccessJob"


class FailJobImageNotFound(DockerTask):
    image = "image-does-not-exists"
    name = "FailJobImageNotFound"

class FailJobContainer(DockerTask):
    image = "yikaus/alpine-bash"
    name = "FailJobContainer"
    command = ['/bin/bash','-c','"exit 1"']

class WriteToTmpDir(DockerTask):
    image = "yikaus/alpine-bash"
    name = "WriteToTmpDir"
    command = ['/bin/bash', '-c', '"echo test > $LUIGI_TMP_DIR/test"']

class MountLocalFileAsVolume(DockerTask):

    image = "yikaus/alpine-bash"
    name = "MountLocalFileAsVolume"
    # volumes= {'/tmp/local_file_test': {'bind': local_file, 'mode': 'rw'}}
    volumes=[local_file+':/tmp/local_file_test']

    command = ['/bin/bash', '-c', '"cat /tmp/local_file_test"']



class TestDockerTask(unittest.TestCase):

    def test_success_job(self):
        success = luigi.run(["SuccessJob", "--local-scheduler"])
        self.assertTrue(success)

    def test_temp_file_creation(self):
        writedir = WriteToTmpDir()
        success = writedir.run()
        success=True
        self.assertTrue(success)

    def test_local_file_mount(self):
        localfile = MountLocalFileAsVolume()
        success = localfile.run()

    def test_fail_job_image_not_found(self):
        fail = FailJobImageNotFound()
        self.assertRaises(ImageNotFound, fail.run)

    def test_fail_job_container(self):
        fail = FailJobContainer()
        self.assertRaises(ContainerError, fail.run)
