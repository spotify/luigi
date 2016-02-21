import luigi
import mock
import random
import six
import unittest

from luigi.contrib.opener import OpenerTarget, NoOpenerError
from luigi.mock import MockTarget
from luigi.file import LocalTarget


class TestOpenerTarget(unittest.TestCase):

    def setUp(self):
        MockTarget.fs.clear()

        self.local_file = '/tmp/{}/xyz/test.txt'.format(
            random.randint(0, 999999999)
        )

        if LocalTarget.fs.exists(self.local_file):
            LocalTarget.fs.remove(self.local_file)

    def tearDown(self):
        if LocalTarget.fs.exists(self.local_file):
            LocalTarget.fs.remove(self.local_file)

    def test_invalid_target(self):
        '''Verify invalid types raises NoOpenerError

        '''
        self.assertRaises(NoOpenerError, OpenerTarget, 'foo://bar.txt')

    def test_mock_target(self):
        '''Verify mock target url

        '''
        target = OpenerTarget('mock://foo/bar.txt')
        self.assertEqual(type(target), MockTarget)

        # Write to the target
        target.open('w').close()
        self.assertTrue(MockTarget.fs.exists('foo/bar.txt'))

    def test_mock_target_root(self):
        '''Verify mock target url

        '''
        target = OpenerTarget('mock:///foo/bar.txt')
        self.assertEqual(type(target), MockTarget)

        # Write to the target
        target.open('w').close()
        self.assertTrue(MockTarget.fs.exists('/foo/bar.txt'))

    def test_default_target(self):
        '''Verify default local target url

        '''
        target = OpenerTarget(self.local_file)
        self.assertEqual(type(target), LocalTarget)

        # Write to the target
        target.open('w').close()
        self.assertTrue(LocalTarget.fs.exists(self.local_file))

    def test_local_target(self):
        '''Verify basic local target url

        '''
        local_file = "file://{}".format(self.local_file)
        target = OpenerTarget(local_file)
        self.assertEqual(type(target), LocalTarget)

        # Write to the target
        target.open('w').close()
        self.assertTrue(LocalTarget.fs.exists(self.local_file))

    @mock.patch('luigi.file.LocalTarget.__init__')
    @mock.patch('luigi.file.LocalTarget.__del__')
    def test_local_tmp_target(self, lt_del_patch, lt_init_patch):
        '''Verify local target url with query string

        '''
        lt_init_patch.return_value = None
        lt_del_patch.return_value = None

        local_file = "file://{}?is_tmp".format(self.local_file)
        OpenerTarget(local_file)
        lt_init_patch.assert_called_with(self.local_file, is_tmp=True)

    @mock.patch('luigi.s3.S3Target.__init__')
    def test_s3_parse(self, s3_init_patch):
        '''Verify basic s3 target url

        '''
        s3_init_patch.return_value = None

        local_file = "s3://zefr/foo/bar.txt"
        OpenerTarget(local_file)
        s3_init_patch.assert_called_with("s3://zefr/foo/bar.txt")

    @mock.patch('luigi.s3.S3Target.__init__')
    def test_s3_parse_param(self, s3_init_patch):
        '''Verify s3 target url with params

        '''
        s3_init_patch.return_value = None

        local_file = "s3://zefr/foo/bar.txt?foo=hello&bar=true"
        OpenerTarget(local_file)
        s3_init_patch.assert_called_with("s3://zefr/foo/bar.txt",
                                         foo='hello',
                                         bar='true')

    def test_binary_support(self):
        '''Make sure keyword arguments are preserved through the OpenerTarget

        '''
        if six.PY3:
            # Verify we can't normally write binary data
            fp = OpenerTarget("mock://file.txt").open('w')
            self.assertRaises(TypeError, fp.write, b'\x07\x08\x07')

            # Verify the format is passed to the target and write binary data
            fp = OpenerTarget("mock://file.txt",
                              format=luigi.format.MixedUnicodeBytes).open('w')
            fp.write(b'\x07\x08\x07')
            fp.close()
