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
import os
import os.path
import random
import tempfile
import urlparse

import boto
from boto.s3.key import Key

import configuration
from ConfigParser import NoSectionError, NoOptionError
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
            aws_access_key_id = self._get_s3_config('aws_access_key_id')
        if not aws_secret_access_key:
            aws_secret_access_key = self._get_s3_config('aws_secret_access_key')

        self.s3 = boto.connect_s3(aws_access_key_id,
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
        
        logger.debug('Path %s does not exist', path)
        return False
    
    def remove(self, path, recursive=True):
        """
        Remove a file or directory from S3.
        """
        if not self.exists(path):
            logger.debug('Could not delete %s; path does not exist', path)
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
            logger.debug('Deleting %s from bucket %s', key, bucket)
            return True

        if self.is_dir(path) and not recursive:
            raise InvalidDeleteException('Path %s is a directory. Must use recursive delete' % path)

        delete_key_list = [k for k in s3_bucket.list(self._add_path_delimiter(key))]

        if len(delete_key_list) > 0:
            for k in delete_key_list:
                logger.debug('Deleting %s from bucket %s', k, bucket)
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

    def copy(self, source_path, destination_path):
        """
        Copy an object from one S3 location to another.
        """
        (src_bucket, src_key) = self._path_to_bucket_and_key(source_path)
        (dst_bucket, dst_key) = self._path_to_bucket_and_key(destination_path)

        s3_bucket = self.s3.get_bucket(dst_bucket, validate=True)

        if self.is_dir(source_path):
            src_prefix = self._add_path_delimiter(src_key)
            dst_prefix = self._add_path_delimiter(dst_key)
            for key in self.list(source_path):
                s3_bucket.copy_key(dst_prefix + key,
                                   src_bucket,
                                   src_prefix + key)
        else:
            s3_bucket.copy_key(dst_key, src_bucket, src_key)

    def rename(self, source_path, destination_path):
        """
        Rename/move an object from one S3 location to another.
        """
        self.copy(source_path, destination_path)
        self.remove(source_path)

    def list(self, path):
        """
        Get an iterable with S3 folder contents.
        Iterable contains paths relative to queried path.
        """
        (bucket, key) = self._path_to_bucket_and_key(path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        key_path = self._add_path_delimiter(key)
        key_path_len = len(key_path)
        for item in s3_bucket.list(prefix=key_path):
            yield item.key[key_path_len:]

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

    def _get_s3_config(self, key):
        try:
            return configuration.get_config().get('s3', key)
        except NoSectionError:
            return None
        except NoOptionError:
            return None

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
        self.buffer = []

    def read(self, size=0):
        return self.s3_key.read(size=size)

    def close(self):
        self.s3_key.close()

    def __del__(self):
        self.close()

    def __exit__(self, exc_type, exc, traceback):
        self.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        self.close()
    
    def _add_to_buffer(self, line):
        self.buffer.append(line)
    
    def _flush_buffer(self):
        output = ''.join(self.buffer)
        self.buffer = []
        return output
    
    def __iter__(self):
        key_iter = self.s3_key.__iter__()
        
        has_next = True
        while has_next:
            try:
                # grab the next chunk
                chunk = key_iter.next()
                
                # split on newlines, preserving the newline
                for line in chunk.splitlines(True):
                    
                    if not line.endswith(os.linesep):
                        # no newline, so store in buffer
                        self._add_to_buffer(line)
                    else:
                        # newline found, send it out
                        if self.buffer:
                            self._add_to_buffer(line)
                            yield self._flush_buffer()
                        else:
                            yield line
            except StopIteration:
                # send out anything we have left in the buffer
                output = self._flush_buffer()
                if output:
                    yield output
                has_next = False
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

class S3EmrTarget(S3Target):
    """
    Defines a target directory for EMR output on S3

    This checks for two things:  that the path exists (just like the S3Target) and that the _SUCCESS file exists
    within the directory.  Because Hadoop outputs into a directory and not a single file, the path is assume to be a
    directory.

    This is meant to be a handy alternative to AtomicS3File.  The AtomicFile approach can be burdensome for S3 since
    there are no directories, per se.  If we have 1,000,000 output files, then we have to rename 1,000,000 objects.
    """

    fs = None

    def __init__(self, path, format=None, client=None):
        if path[-1] is not "/":
            raise ValueError("S3EmrTarget requires the path to be to a directory.  It must end with a slash ( / ).")
        super(S3Target, self).__init__(path)
        self.format = format
        self.fs = client or S3Client()

    def exists(self):
        hadoopSemaphore = self.path + '_SUCCESS'
        return self.fs.exists(hadoopSemaphore)

class S3PathTask(ExternalTask):
    """
    A external task that to require existence of
    a path in S3.
    """
    path = Parameter()
        
    def output(self):
        return S3Target(self.path)

class S3EmrTask(ExternalTask):
    """
    An external task that requires the existence of EMR output in S3
    """
    path = Parameter()

    def output(self):
        return S3EmrTarget(self.path)