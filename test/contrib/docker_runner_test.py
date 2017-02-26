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



import unittest
import luigi
import logging
from luigi.contrib.docker_runner import DockerTask

logger = logging.getLogger('luigi-interface')

try:
    import docker
    from docker.errors import ContainerError, ImageNotFound
except ImportError:
    raise unittest.SkipTest('docker is not installed. This test requires docker.')


class SuccessJob(DockerTask):
    image = "alpine"
    name = "success"


class FailJobImageNotFound(DockerTask):
    image = "image-does-not-exists"
    name = "failimage"

class FailJobContainer(DockerTask):
    image = "alpine"
    name = "failcontainer"
    command = ['/bin/ash','-c', '" this should fail"']



class TestDockerTask(unittest.TestCase):

    def test_success_job(self):
        success = luigi.run(["SuccessJob", "--local-scheduler"])
        self.assertTrue(success)

    def test_fail_job_image_not_found(self):
        fail = FailJobImageNotFound()
        self.assertRaises(ImageNotFound, fail.run)

    def test_fail_job_container(self):
        fail = FailJobContainer()
        self.assertRaises(ContainerError, fail.run)
