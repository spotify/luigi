# Copyright (c) 2013 Mortar Data
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
import itertools
import logging
import os.path
import random
import tempfile
import urlparse

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import configuration
from luigi.parameter import Parameter
from luigi.target import FileSystem
from luigi.target import FileSystemTarget
from luigi.target import FileSystemException
from luigi.task import ExternalTask

# two different ways of marking a directory
# with a suffix in S3
S3_DIRECTORY_MARKER_SUFFIX_0 = '_$folder$'
S3_DIRECTORY_MARKER_SUFFIX_1 = '/'

logger = logging.getLogger('luigi-interface')

class InvalidDeleteException(FileSystemException):
    pass

class FileNotFoundException(FileSystemException):
    pass

class S3Client(FileSystem):
    """
    boto-powered S3 client.
    """
        
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None):
        if not aws_access_key_id:
            aws_access_key_id = configuration.get_config().get('s3', 'aws_access_key_id')
        if not aws_secret_access_key:
            aws_secret_access_key = configuration.get_config().get('s3', 'aws_secret_access_key')
        
        self.s3 = S3Connection(aws_access_key_id,
                               aws_secret_access_key,
                               is_secure=True)
        
    def exists(self, path):
        """
        Does provided path exist on S3?
        """
        (bucket, key) = self._path_to_bucket_and_key(path)
        
        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # root always exists
        if self._is_root(key):
            return True
        
        # file
        s3_key = s3_bucket.get_key(key)
        if s3_key:
            return True

        if self.is_dir(path):
            return True
        
        logger.debug('Path %s does not exist' % path)
        return False
    
    def remove(self, path, recursive=True):
        """
        Remove a file or directory from S3.
        """
        if not self.exists(path):
            logger.debug('Could not delete %s; path does not exist' % path)
            return False

        (bucket, key) = self._path_to_bucket_and_key(path)

        # root
        if self._is_root(key):
            raise InvalidDeleteException('Cannot delete root of bucket at path %s' % path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # file
        s3_key = s3_bucket.get_key(key)
        if s3_key:
            s3_bucket.delete_key(s3_key)
            logger.debug('Deleting %s from bucket %s' % (key, bucket))
            return True

        if self.is_dir(path) and not recursive:
            raise InvalidDeleteException('Path %s is a directory. Must use recursive delete' % path)

        delete_key_list = [k for k in s3_bucket.list(self._add_path_delimiter(key))]

        if len(delete_key_list) > 0:
            for k in delete_key_list:
                logger.debug('Deleting %s from bucket %s' % (k, bucket))
            s3_bucket.delete_keys(delete_key_list)
            return True
        
        return False

    def get_key(self, path):
        (bucket, key) = self._path_to_bucket_and_key(path)

        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        return s3_bucket.get_key(key)

    def put(self, local_path, destination_s3_path):
        """
        Put an object stored locally to an S3 path.
        """
        (bucket, key) = self._path_to_bucket_and_key(destination_s3_path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # put the file
        s3_key = Key(s3_bucket)
        s3_key.key = key
        s3_key.set_contents_from_filename(local_path)

    def is_dir(self, path):
        """
        Is the parameter S3 path a directory?
        """
        (bucket, key) = self._path_to_bucket_and_key(path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # root is a directory
        if self._is_root(key):
            return True

        for suffix in (S3_DIRECTORY_MARKER_SUFFIX_0, S3_DIRECTORY_MARKER_SUFFIX_1):
            s3_dir_with_suffix_key = s3_bucket.get_key(key + suffix)
            if s3_dir_with_suffix_key:
                return True

        # files with this prefix
        key_path = self._add_path_delimiter(key)
        s3_bucket_list_result = \
            list(itertools.islice(
                    s3_bucket.list(prefix=key_path),
                 1))
        if s3_bucket_list_result:
            return True

        return False

    def _path_to_bucket_and_key(self, path):
        (scheme, netloc, path, query, fragment) = urlparse.urlsplit(path)
        path_without_initial_slash = path[1:]
        return netloc, path_without_initial_slash
    
    def _is_root(self, key):
        return (len(key) == 0) or (key == '/')
    
    def _add_path_delimiter(self, key):
        return key if key[-1:] == '/' else key + '/'

class AtomicS3File(file):
    """
    An S3 file that writes to a temp file and put to S3 on close.
    """
    def __init__(self, path, s3_client):
        self.__tmp_path = \
            os.path.join(tempfile.gettempdir(), 
                         'luigi-s3-tmp-%09d' % random.randrange(0, 1e10))
        self.path = path
        self.s3_client = s3_client
        super(AtomicS3File, self).__init__(self.__tmp_path, 'w')
    
    def close(self):
        """
        Close the file.
        """
        super(AtomicS3File, self).close()

        # store the contents in S3
        self.s3_client.put(self.__tmp_path, self.path)
    
    def __del__(self):
        # remove the temporary directory
        if os.path.exists(self.__tmp_path):
            os.remove(self.__tmp_path)

    def __exit__(self, exc_type, exc, traceback):
        " Close/commit the file if there are no exception "
        if exc_type:
            return
        return file.__exit__(self, exc_type, exc, traceback)

class ReadableS3File(object):

    def __init__(self, s3_key):
        self.s3_key = s3_key

    def read(self, size=0):
        return self.s3_key.read(size=size)

    def close(self):
        self.s3_key.close()

    def __del__(self):
        self.close()

    def __exit__(self, exc_type, exc, traceback):
        self.close()

class S3Target(FileSystemTarget):
    """
    """
    
    fs = None

    def __init__(self, path, format=None, client=None):
        super(S3Target, self).__init__(path)
        self.format = format
        self.fs = client or S3Client()

    def open(self, mode='r'):
        """
        """
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            s3_key = self.fs.get_key(self.path)
            if s3_key:
                return ReadableS3File(s3_key)
            else:
                raise FileNotFoundException("Could not find file at %s" % self.path)
        else:
            return AtomicS3File(self.path, self.fs)

class S3PathTask(ExternalTask):
    """
    A external task that to require existence of
    a path in S3.
    """
    path = Parameter()
        
    def output(self):
        return S3Target(self.path)
