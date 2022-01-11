import bz2
import os
import tempfile
import unittest
import uuid
from datetime import datetime

import pytest

import luigi
from luigi.format import NopFormat

try:
    import dropbox
    import dropbox.exceptions
    from luigi.contrib.dropbox import DropboxClient
except ImportError:
    raise unittest.SkipTest('DropboxTarget and DropboxClient will not be tested. Dropbox library is not installed')

DROPBOX_APP_TOKEN = os.environ.get('DROPBOX_APP_TOKEN')

if not DROPBOX_APP_TOKEN:
    raise ValueError("In order to test DropboxTarget and DropboxClient, the DROPBOX_APP_TOKEN environment variable "
                     "must contain a valid Dropbox OAuth2 Token. \n"
                     "Get one at https://www.dropbox.com/developers/apps ")

DROPBOX_TEST_PATH = "/luigi-tests/luigi-test-" + datetime.now().strftime("%Y.%m.%d-%H.%M.%S") + str(uuid.uuid4())

# These paths will be created in the test set-up
DROPBOX_TEST_SIMPLE_DIR = DROPBOX_TEST_PATH + '/dir2'
DROPBOX_TEST_FILE_IN_DIR = DROPBOX_TEST_SIMPLE_DIR + '/test2.txt'
DROPBOX_TEST_SIMPLE_FILE = DROPBOX_TEST_PATH + '/test.txt'
DROPBOX_TEST_DIR_TO_DELETE = DROPBOX_TEST_PATH + '/dir_to_delete'
DROPBOX_TEST_FILE_TO_DELETE_2 = DROPBOX_TEST_DIR_TO_DELETE + '/test3.2.txt'
DROPBOX_TEST_FILE_TO_DELETE_1 = DROPBOX_TEST_DIR_TO_DELETE + '/test3.1.txt'
DROPBOX_TEST_FILE_TO_COPY_ORIG = DROPBOX_TEST_PATH + '/dir4/test4.txt'
DROPBOX_TEST_FILE_TO_MOVE_ORIG = DROPBOX_TEST_PATH + '/dir3/test3.txt'

# All the following paths will be used by the tests
DROPBOX_TEST_SMALL_FILE = DROPBOX_TEST_PATH + '/dir/small.txt'
DROPBOX_TEST_LARGE_FILE = DROPBOX_TEST_PATH + '/dir/big.bin'

DROPBOX_TEST_FILE_TO_COPY_DEST = DROPBOX_TEST_PATH + '/dir_four/test_four.txt'

DROPBOX_TEST_FILE_TO_MOVE_DEST = DROPBOX_TEST_PATH + '/dir_three/test_three.txt'
DROPBOX_TEST_OUTER_DIR_TO_CREATE = DROPBOX_TEST_PATH + '/new_folder'
DROPBOX_TEST_DIR_TO_CREATE = DROPBOX_TEST_OUTER_DIR_TO_CREATE + '/inner_folder'

DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE = DROPBOX_TEST_PATH + '/another_new_folder'

DROPBOX_TEST_FILE_TO_UPLOAD_BZIP2 = DROPBOX_TEST_PATH + '/bin.file'
DROPBOX_TEST_FILE_TO_UPLOAD_TEXT = DROPBOX_TEST_PATH + '/text.txt'
DROPBOX_TEST_FILE_TO_UPLOAD_BIN = DROPBOX_TEST_PATH + '/file.bin'
DROPBOX_TEST_FILE_TO_UPLOAD_LARGE = DROPBOX_TEST_PATH + '/file.blob'

DROPBOX_TEST_NON_EXISTING_FILE = DROPBOX_TEST_SIMPLE_DIR + 'ajdlkajfal'


@pytest.mark.dropbox
class TestClientDropbox(unittest.TestCase):
    def setUp(self):
        self.luigiconn = DropboxClient(DROPBOX_APP_TOKEN)

        self.dropbox_api = dropbox.dropbox.Dropbox(DROPBOX_APP_TOKEN)
        self.dropbox_api.files_upload(b'hello', DROPBOX_TEST_SIMPLE_FILE)
        self.dropbox_api.files_upload(b'hello2', DROPBOX_TEST_FILE_IN_DIR)
        self.dropbox_api.files_upload(b'hello3', DROPBOX_TEST_FILE_TO_MOVE_ORIG)
        self.dropbox_api.files_upload(b'hello4', DROPBOX_TEST_FILE_TO_COPY_ORIG)
        self.dropbox_api.files_upload(b'hello3.1', DROPBOX_TEST_FILE_TO_DELETE_1)
        self.dropbox_api.files_upload(b'hello3.2', DROPBOX_TEST_FILE_TO_DELETE_2)

    def tearDown(self):
        self.dropbox_api.files_delete_v2(DROPBOX_TEST_PATH)
        self.dropbox_api._session.close()

    def test_exists(self):
        self.assertTrue(self.luigiconn.exists('/'))
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_PATH))
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_SIMPLE_DIR))
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_SIMPLE_DIR + '/'))
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_SIMPLE_FILE))

        self.assertFalse(self.luigiconn.exists(DROPBOX_TEST_SIMPLE_FILE + '/'))
        self.assertFalse(self.luigiconn.exists(DROPBOX_TEST_NON_EXISTING_FILE))

    def test_listdir_simple(self):
        list_of_dirs = self.luigiconn.listdir(DROPBOX_TEST_PATH)
        self.assertTrue('/' not in list_of_dirs)
        self.assertTrue(DROPBOX_TEST_PATH in list_of_dirs)
        self.assertTrue(DROPBOX_TEST_SIMPLE_FILE in list_of_dirs)  # we verify recursivity

    def test_listdir_simple_with_one_slash(self):
        list_of_dirs = self.luigiconn.listdir(DROPBOX_TEST_PATH + '/')
        self.assertTrue('/' not in list_of_dirs)
        self.assertTrue(DROPBOX_TEST_PATH in list_of_dirs)
        self.assertTrue(DROPBOX_TEST_SIMPLE_FILE in list_of_dirs)  # we verify recursivity

    def test_listdir_multiple(self):
        list_of_dirs = self.luigiconn.listdir(DROPBOX_TEST_PATH, limit=2)
        self.assertTrue('/' not in list_of_dirs)
        self.assertTrue(DROPBOX_TEST_PATH in list_of_dirs)
        self.assertTrue(DROPBOX_TEST_SIMPLE_FILE in list_of_dirs)  # we verify recursivity

    def test_listdir_nonexisting(self):
        with self.assertRaises(dropbox.exceptions.ApiError):
            self.luigiconn.listdir(DROPBOX_TEST_NON_EXISTING_FILE)

    def test_remove(self):
        # We remove File_to_delete_1. We make sure it is the only file that gets deleted
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_FILE_TO_DELETE_1))
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_FILE_TO_DELETE_2))
        self.assertTrue(self.luigiconn.remove(DROPBOX_TEST_FILE_TO_DELETE_1))
        self.assertFalse(self.luigiconn.exists(DROPBOX_TEST_FILE_TO_DELETE_1))
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_FILE_TO_DELETE_2))

        # We remove a directory, we make sure that the files that were in the directory are also deleted
        self.luigiconn.remove(DROPBOX_TEST_DIR_TO_DELETE)
        self.assertFalse(self.luigiconn.exists(DROPBOX_TEST_FILE_TO_DELETE_2))

        # We make sure that we return False when we fail to remove a non-existing path
        self.assertFalse(self.luigiconn.remove(DROPBOX_TEST_NON_EXISTING_FILE))
        self.assertFalse(self.luigiconn.remove(DROPBOX_TEST_NON_EXISTING_FILE + '/'))

    def test_mkdir_new_dir(self):
        self.assertFalse(self.luigiconn.exists(DROPBOX_TEST_DIR_TO_CREATE))
        self.assertFalse(self.luigiconn.exists(DROPBOX_TEST_OUTER_DIR_TO_CREATE))
        self.luigiconn.mkdir(DROPBOX_TEST_DIR_TO_CREATE)
        self.assertTrue(self.luigiconn.isdir(DROPBOX_TEST_OUTER_DIR_TO_CREATE))
        self.assertTrue(self.luigiconn.isdir(DROPBOX_TEST_DIR_TO_CREATE))
        self.assertTrue(self.luigiconn.isdir(DROPBOX_TEST_DIR_TO_CREATE))

    def aux_lifecycle_of_directory(self, path):
        # Initially, the directory does not exists
        self.assertFalse(self.luigiconn.exists(path))
        self.assertFalse(self.luigiconn.isdir(path))

        # Now we create the directory and verify that it exists
        self.luigiconn.mkdir(path)
        self.assertTrue(self.luigiconn.exists(path))
        self.assertTrue(self.luigiconn.isdir(path))

        # Now we remote the directory and verify that it no longer exists
        self.luigiconn.remove(path)
        self.assertFalse(self.luigiconn.exists(path))
        self.assertFalse(self.luigiconn.isdir(path))

    def test_lifecycle_of_dirpath(self):
        self.aux_lifecycle_of_directory(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE)

    def test_lifecycle_of_dirpath_with_trailing_slash(self):
        self.aux_lifecycle_of_directory(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE + '/')

    def test_lifecycle_of_dirpath_with_several_trailing_mixed(self):
        self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE + '/')
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE))
        self.luigiconn.remove(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE)
        self.assertFalse(self.luigiconn.exists(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE + '/'))

    def test_lifecycle_of_dirpath_with_several_trailing_mixed_2(self):
        self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE)
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE + '/'))
        self.luigiconn.remove(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE + '/')
        self.assertFalse(self.luigiconn.exists(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE))

    def test_mkdir_new_dir_two_slashes(self):
        with self.assertRaises(dropbox.dropbox.ApiError):
            self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_DIR_TO_CREATE_AND_DELETE + '//')

    def test_mkdir_recreate_dir(self):
        try:
            self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_DIR)
        except Exception as ex:
            self.fail("mkdir with default options raises Exception:" + str(ex))

        try:
            self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_DIR, raise_if_exists=False)
        except Exception as ex:
            self.fail("mkdir with 'raise_if_exists=False' raises Exception:" + str(ex))

        with self.assertRaises(luigi.target.FileAlreadyExists):
            self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_DIR, raise_if_exists=True)

    def test_mkdir_recreate_slashed_dir(self):
        try:
            self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_DIR + '/')
        except Exception as ex:
            self.fail("mkdir with default options raises Exception:" + str(ex))

        try:
            self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_DIR + '/', raise_if_exists=False)
        except Exception as ex:
            self.fail("mkdir with 'raise_if_exists=False' raises Exception:" + str(ex))

        with self.assertRaises(luigi.target.FileAlreadyExists):
            self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_DIR + '/', raise_if_exists=True)

    def test_mkdir_recreate_file(self):
        with self.assertRaises(luigi.target.NotADirectory):
            self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_FILE)

        with self.assertRaises(luigi.target.NotADirectory):
            self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_FILE, raise_if_exists=True)

        with self.assertRaises(luigi.target.NotADirectory):
            self.luigiconn.mkdir(DROPBOX_TEST_SIMPLE_FILE, raise_if_exists=False)

    def test_isdir(self):
        self.assertTrue(self.luigiconn.isdir('/'))
        self.assertTrue(self.luigiconn.isdir(DROPBOX_TEST_PATH))
        self.assertTrue(self.luigiconn.isdir(DROPBOX_TEST_SIMPLE_DIR))
        self.assertTrue(self.luigiconn.isdir(DROPBOX_TEST_SIMPLE_DIR + '/'))

        self.assertFalse(self.luigiconn.isdir(DROPBOX_TEST_SIMPLE_FILE))
        self.assertFalse(self.luigiconn.isdir(DROPBOX_TEST_NON_EXISTING_FILE))
        self.assertFalse(self.luigiconn.isdir(DROPBOX_TEST_NON_EXISTING_FILE + '/'))

    def test_move(self):
        md, res = self.dropbox_api.files_download(DROPBOX_TEST_FILE_TO_MOVE_ORIG)
        initial_contents = res.content

        self.luigiconn.move(DROPBOX_TEST_FILE_TO_MOVE_ORIG, DROPBOX_TEST_FILE_TO_MOVE_DEST)

        md, res = self.dropbox_api.files_download(DROPBOX_TEST_FILE_TO_MOVE_DEST)
        after_moving_contents = res.content

        self.assertEqual(initial_contents, after_moving_contents)
        self.assertFalse(self.luigiconn.exists(DROPBOX_TEST_FILE_TO_MOVE_ORIG))
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_FILE_TO_MOVE_DEST))

    def test_copy(self):
        md, res = self.dropbox_api.files_download(DROPBOX_TEST_FILE_TO_COPY_ORIG)
        initial_contents = res.content

        self.luigiconn.copy(DROPBOX_TEST_FILE_TO_COPY_ORIG, DROPBOX_TEST_FILE_TO_COPY_DEST)

        md, res = self.dropbox_api.files_download(DROPBOX_TEST_FILE_TO_COPY_DEST)
        after_copyng_contents = res.content

        self.assertEqual(initial_contents, after_copyng_contents)
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_FILE_TO_COPY_ORIG))
        self.assertTrue(self.luigiconn.exists(DROPBOX_TEST_FILE_TO_COPY_DEST))


@pytest.mark.dropbox
class TestDropboxTarget(unittest.TestCase):
    def setUp(self):
        self.luigiconn = DropboxClient(DROPBOX_APP_TOKEN)
        self.dropbox_api = dropbox.dropbox.Dropbox(DROPBOX_APP_TOKEN)

        self.initial_contents = b'\x00hello\xff\x00-\xe2\x82\x28'  # Binary invalid-utf8 sequence
        self.dropbox_api.files_upload(self.initial_contents, DROPBOX_TEST_SIMPLE_FILE)

    def tearDown(self):
        self.dropbox_api.files_delete_v2(DROPBOX_TEST_PATH)
        self.dropbox_api._session.close()

    def test_download_from_dropboxtarget_to_local(self):
        class Download(luigi.ExternalTask):
            dbx_path = luigi.Parameter()

            def output(self):
                return luigi.contrib.dropbox.DropboxTarget(self.dbx_path, DROPBOX_APP_TOKEN, format=NopFormat())

        class DbxToLocalTask(luigi.Task):
            local_path = luigi.Parameter()
            dbx_path = luigi.Parameter()

            def requires(self):
                return Download(dbx_path=self.dbx_path)

            def output(self):
                return luigi.LocalTarget(path=self.local_path, format=NopFormat())

            def run(self):
                with self.input().open('r') as dbxfile, self.output().open('w') as localfile:
                    remote_contents = dbxfile.read()
                    localfile.write(remote_contents * 3)

        tmp_file = tempfile.mkdtemp() + os.sep + "tmp.file"
        luigi.build([DbxToLocalTask(dbx_path=DROPBOX_TEST_SIMPLE_FILE, local_path=tmp_file)],
                    local_scheduler=True)

        expected_contents = self.initial_contents * 3
        with open(tmp_file, 'rb') as f:
            actual_contents = f.read()

        self.assertEqual(expected_contents, actual_contents)

    def test_write_small_text_file_to_dropbox(self):
        small_input_text = "The greatest glory in living lies not in never falling\nbut in rising every time we fall."

        class WriteToDrobopxTest(luigi.Task):
            def output(self):
                return luigi.contrib.dropbox.DropboxTarget(DROPBOX_TEST_FILE_TO_UPLOAD_TEXT, DROPBOX_APP_TOKEN)

            def run(self):
                with self.output().open('w') as dbxfile:
                    dbxfile.write(small_input_text)

        luigi.build([WriteToDrobopxTest()], local_scheduler=True)
        actual_content = self.dropbox_api.files_download(DROPBOX_TEST_FILE_TO_UPLOAD_TEXT)[1].content
        self.assertEqual(actual_content.decode(), small_input_text)

    def aux_write_binary_file_to_dropbox(self, multiplier):
        large_contents = b"X\n\xe2\x28\xa1" * multiplier
        output_file = DROPBOX_TEST_FILE_TO_UPLOAD_LARGE + str(multiplier)

        class WriteToDrobopxTest(luigi.Task):
            def output(self):
                return luigi.contrib.dropbox.DropboxTarget(output_file, DROPBOX_APP_TOKEN,
                                                           format=luigi.format.Nop)

            def run(self):
                with self.output().open('w') as dbxfile:
                    dbxfile.write(large_contents)

        luigi.build([WriteToDrobopxTest()], local_scheduler=True)
        actual_content = self.dropbox_api.files_download(output_file)[1].content
        self.assertEqual(actual_content, large_contents)

    def test_write_small_binary_file_to_dropbox(self):
        self.aux_write_binary_file_to_dropbox(1024)

    def test_write_medium_binary_file_to_dropbox(self):
        self.aux_write_binary_file_to_dropbox(1024 * 1024)

    def test_write_large_binary_file_to_dropbox(self):
        self.aux_write_binary_file_to_dropbox(3 * 1024 * 1024)

    def test_write_using_nondefault_format(self):
        contents = b"X\n\xe2\x28\xa1"

        class WriteToDrobopxTest(luigi.Task):
            def output(self):
                return luigi.contrib.dropbox.DropboxTarget(DROPBOX_TEST_FILE_TO_UPLOAD_BZIP2, DROPBOX_APP_TOKEN,
                                                           format=luigi.format.Bzip2)

            def run(self):
                with self.output().open('w') as bzip2_dbxfile:
                    bzip2_dbxfile.write(contents)

        luigi.build([WriteToDrobopxTest()], local_scheduler=True)

        remote_content = self.dropbox_api.files_download(DROPBOX_TEST_FILE_TO_UPLOAD_BZIP2)[1].content
        self.assertEqual(contents, bz2.decompress(remote_content))

    def test_write_using_a_temporary_path(self):
        contents = b"X\n\xe2\x28\xa1"

        class WriteToDrobopxTest(luigi.Task):
            def output(self):
                return luigi.contrib.dropbox.DropboxTarget(DROPBOX_TEST_FILE_TO_UPLOAD_BIN, DROPBOX_APP_TOKEN)

            def run(self):
                with self.output().temporary_path() as tmp_path:
                    open(tmp_path, 'wb').write(contents)

        luigi.build([WriteToDrobopxTest()], local_scheduler=True)
        actual_content = self.dropbox_api.files_download(DROPBOX_TEST_FILE_TO_UPLOAD_BIN)[1].content
        self.assertEqual(actual_content, contents)
