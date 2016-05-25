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
Tests for the Luigi Docker wrapper.

Requires:

- docker-py package
"""

import logging
import mock
import unittest
import uuid

import luigi
from luigi.contrib.docker import DockerImageTarget, DockerImageBuildTask

logger = logging.getLogger('luigi-interface')

try:
    import docker
    import docker.utils
    import docker.errors
except ImportError:
    no_docker = True
    logger.warning("Failed to import docker, will run mocked tests only")
else:
    try:
        client = docker.Client(**docker.utils.kwargs_from_env(assert_hostname=False))
        no_docker = False
    except docker.errors.Exception:
        no_docker = True
        logger.warning("Failed to connect to docker, will run mocked tests only")


class DummyNotFound(Exception):
    def __init__(self):
        pass


class DockerMocked(object):
    """
    Adds some minimal non-regression tests even if
    no docker host can be reached. In such a case,
    expect the docker API to behave exactly as desribed
    in the documentation
    """

    def setUp(self):
        self.client = mock.MagicMock()
        self.docker = mock.MagicMock()
        self.docker.errors = mock.MagicMock()
        self.docker.errors.NotFound = DummyNotFound
        self.docker.utils = mock.MagicMock()
        self.docker.Client.return_value = self.client
        self.patches = []
        self.patches.append(mock.patch.dict('sys.modules', docker=self.docker))
        for patch in self.patches:
            patch.start()
        reload(luigi.contrib.docker)

    def tearDown(self):
        for patch in self.patches:
            patch.stop()
        reload(luigi.contrib.docker)


class TestDockerImageTarget(DockerMocked, unittest.TestCase):

    def test_exists(self):

        self.docker.utils.kwargs_from_env.return_value = {}
        self.client.inspect_image.return_value = {}

        target = luigi.contrib.docker.DockerImageTarget('dummy name')
        self.assertTrue(target.exists())

        self.client.inspect_image.side_effect = DummyNotFound
        self.assertFalse(target.exists())

    def test_image_name(self):
        target = DockerImageTarget("some.name")
        self.assertEquals(target.image_name, "some.name:latest")
        target = DockerImageTarget("some.other.name", tag="tagged")
        self.assertEquals(target.image_name, "some.other.name:tagged")

        target = DockerImageTarget("some.name", registry="my.docker-registry")
        self.assertEquals(target.image_name, "my.docker-registry/some.name:latest")
        target = DockerImageTarget("some.other.name", tag="tagged", registry="my.docker-registry")
        self.assertEquals(target.image_name, "my.docker-registry/some.other.name:tagged")


class TestDockerImageBuildTask(DockerMocked, unittest.TestCase):

    def test_run(self):
        DockerImageBuildTask(name='my.image').run()
        self.client.build.assert_called_once_with(path='.', tag='my.image:latest', dockerfile='Dockerfile')
        self.client.build.reset_mock()

        DockerImageBuildTask(name='my.image', dockerfile='other').run()
        self.client.build.assert_called_once_with(path='.', tag='my.image:latest', dockerfile='other')
        self.client.build.reset_mock()

        DockerImageBuildTask(name='my.image', dockerfile='other', path='/build-root').run()
        self.client.build.assert_called_once_with(path='/build-root', tag='my.image:latest', dockerfile='other')
        self.client.build.reset_mock()


@unittest.skipIf(no_docker, "Failed to import/connect to docker")
class TestDockerImageTargetIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # In case there is no docker client (the default in CI),
        # tests will be marked as skept but the setup seems sot be
        # done anyway. Prevent from client not found exception
        cls.pulled = False
        if not no_docker:
            for image in client.images():
                cls.image_id = image['Id']
                break
            else:
                cls.pulled = True
                client.pull('alpine:3.1')
                cls.image_id = client.inspect_image('alpine:3.1')['Id']

    @classmethod
    def tearDownClass(cls):
        if cls.pulled:
            client.remove_image(cls.image_id)

    def setUp(self):
        self.image_name = str(uuid.uuid1())
        self.registry = str(uuid.uuid1())
        self.tag = str(uuid.uuid1())

    def tearDown(self):
        for name in [
            "%s/%s:%s" % (self.registry, self.image_name, self.tag),
            "%s:%s" % (self.image_name, self.tag),
            "%s/%s" % (self.registry, self.image_name),
            self.image_name,
        ]:
            try:
                client.remove_image(name)
            except docker.errors.NotFound:
                pass

    def test_non_existing_image(self):
        self.assertFalse(DockerImageTarget(self.image_name).exists())
        self.assertFalse(DockerImageTarget(self.image_name, tag=self.tag).exists())
        self.assertFalse(DockerImageTarget(self.image_name, registry=self.registry).exists())
        self.assertFalse(DockerImageTarget(self.image_name, registry=self.registry, tag=self.tag).exists())

    def test_existing_image(self):
        client.tag(self.image_id, self.image_name)
        client.tag(self.image_id, self.image_name, self.tag)
        client.tag(self.image_id, "%s/%s" % (self.registry, self.image_name))
        client.tag(self.image_id, "%s/%s" % (self.registry, self.image_name), self.tag)
        self.assertTrue(DockerImageTarget(self.image_name).exists())
        self.assertTrue(DockerImageTarget(self.image_name, tag=self.tag).exists())
        self.assertTrue(DockerImageTarget(self.image_name, registry=self.registry).exists())
        self.assertTrue(DockerImageTarget(self.image_name, registry=self.registry, tag=self.tag).exists())
