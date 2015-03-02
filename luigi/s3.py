# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
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
Implementation of Simple Storage Service support.
:py:class:`S3Target` is a subclass of the Target class to support S3 file system operations
"""

from __future__ import division

import itertools
import logging
import os
import os.path
import sys
try:
    from urlparse import urlsplit
except ImportError:
    from urllib.parse import urlsplit
import warnings
try:
    from ConfigParser import NoSectionError
except ImportError:
    from configparser import NoSectionError

from luigi import six
from luigi.six.moves import range

from luigi import configuration
from luigi.format import FileWrapper, get_default_format, MixedUnicodeBytes
from luigi.parameter import Parameter
from luigi.target import FileSystem, FileSystemException, FileSystemTarget, AtomicLocalFile
from luigi.task import ExternalTask

logger = logging.getLogger('luigi-interface')

try:
    import boto
    from boto.s3.key import Key
except ImportError:
    logger.warning("Loading s3 module without boto installed. Will crash at "
                   "runtime if s3 functionality is used.")


# two different ways of marking a directory
# with a suffix in S3
S3_DIRECTORY_MARKER_SUFFIX_0 = '_$folder$'
S3_DIRECTORY_MARKER_SUFFIX_1 = '/'


class InvalidDeleteException(FileSystemException):
    pass


class FileNotFoundException(FileSystemException):
    pass


class S3Client(FileSystem):
    """
    boto-powered S3 client.
    """

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 **kwargs):
        options = self._get_s3_config()
        options.update(kwargs)
        # Removing key args would break backwards compability
        if not aws_access_key_id:
            aws_access_key_id = options.get('aws_access_key_id')
        if not aws_secret_access_key:
            aws_secret_access_key = options.get('aws_secret_access_key')
        for key in ['aws_access_key_id', 'aws_secret_access_key']:
            if key in options:
                options.pop(key)
        self.s3 = boto.connect_s3(aws_access_key_id,
                                  aws_secret_access_key,
                                  **options)

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
            raise InvalidDeleteException(
                'Cannot delete root of bucket at path %s' % path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # file
        s3_key = s3_bucket.get_key(key)
        if s3_key:
            s3_bucket.delete_key(s3_key)
            logger.debug('Deleting %s from bucket %s', key, bucket)
            return True

        if self.is_dir(path) and not recursive:
            raise InvalidDeleteException(
                'Path %s is a directory. Must use recursive delete' % path)

        delete_key_list = [
            k for k in s3_bucket.list(self._add_path_delimiter(key))]

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

    def put_string(self, content, destination_s3_path):
        """
        Put a string to an S3 path.
        """
        (bucket, key) = self._path_to_bucket_and_key(destination_s3_path)
        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # put the content
        s3_key = Key(s3_bucket)
        s3_key.key = key
        s3_key.set_contents_from_string(content)

    def put_multipart(self, local_path, destination_s3_path, part_size=67108864):
        """
        Put an object stored locally to an S3 path
        using S3 multi-part upload (for files > 5GB).

        :param local_path: Path to source local file
        :param destination_s3_path: URL for target S3 location
        :param part_size: Part size in bytes. Default: 67108864 (64MB), must be >= 5MB and <= 5 GB.
        """
        # calculate number of parts to upload
        # based on the size of the file
        source_size = os.stat(local_path).st_size

        if source_size <= part_size:
            # fallback to standard, non-multipart strategy
            return self.put(local_path, destination_s3_path)

        (bucket, key) = self._path_to_bucket_and_key(destination_s3_path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # calculate the number of parts (int division).
        # use modulo to avoid float precision issues
        # for exactly-sized fits
        num_parts = \
            (source_size // part_size) \
            if source_size % part_size == 0 \
            else (source_size // part_size) + 1

        mp = None
        try:
            mp = s3_bucket.initiate_multipart_upload(key)

            for i in range(num_parts):
                # upload a part at a time to S3
                offset = part_size * i
                bytes = min(part_size, source_size - offset)
                with open(local_path, 'rb') as fp:
                    part_num = i + 1
                    logger.info('Uploading part %s/%s to %s', part_num, num_parts, destination_s3_path)
                    fp.seek(offset)
                    mp.upload_part_from_file(fp, part_num=part_num, size=bytes)

            # finish the upload, making the file available in S3
            mp.complete_upload()
        except BaseException:
            if mp:
                logger.info('Canceling multipart s3 upload for %s', destination_s3_path)
                # cancel the upload so we don't get charged for
                # storage consumed by uploaded parts
                mp.cancel_upload()
            raise

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

        for suffix in (S3_DIRECTORY_MARKER_SUFFIX_0,
                       S3_DIRECTORY_MARKER_SUFFIX_1):
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

    def _get_s3_config(self, key=None):
        try:
            config = dict(configuration.get_config().items('s3'))
        except NoSectionError:
            return {}
        # So what ports etc can be read without us having to specify all dtypes
        for k, v in six.iteritems(config):
            try:
                config[k] = int(v)
            except ValueError:
                pass
        if key:
            return config.get(key)
        return config

    def _path_to_bucket_and_key(self, path):
        (scheme, netloc, path, query, fragment) = urlsplit(path)
        path_without_initial_slash = path[1:]
        return netloc, path_without_initial_slash

    def _is_root(self, key):
        return (len(key) == 0) or (key == '/')

    def _add_path_delimiter(self, key):
        return key if key[-1:] == '/' else key + '/'


class AtomicS3File(AtomicLocalFile):
    """
    An S3 file that writes to a temp file and put to S3 on close.
    """

    def __init__(self, path, s3_client):
        self.s3_client = s3_client
        super(AtomicS3File, self).__init__(path)

    def move_to_final_destination(self):
        self.s3_client.put_multipart(self.tmp_path, self.path)


class ReadableS3File(object):

    def __init__(self, s3_key):
        self.s3_key = s3_key
        self.buffer = []
        self.closed = False
        self.finished = False

    def read(self, size=0):
        f = self.s3_key.read(size=size)

        # boto will loop on the key forever and it's not what is expected by
        # the python io interface
        # boto/boto#2805
        if f == b'':
            self.finished = True
        if self.finished:
            return b''

        return f

    def close(self):
        self.s3_key.close()
        self.closed = True

    def __del__(self):
        self.close()

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __enter__(self):
        return self

    def _add_to_buffer(self, line):
        self.buffer.append(line)

    def _flush_buffer(self):
        output = b''.join(self.buffer)
        self.buffer = []
        return output

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False

    def __iter__(self):
        key_iter = self.s3_key.__iter__()

        has_next = True
        while has_next:
            try:
                # grab the next chunk
                chunk = next(key_iter)

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
        if format is None:
            format = get_default_format()

        # Allow to write unicode in file for retrocompatibility
        if sys.version_info[:2] <= (2, 6):
            format = format >> MixedUnicodeBytes

        self.format = format
        self.fs = client or S3Client()

    def open(self, mode='r'):
        """
        """
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            s3_key = self.fs.get_key(self.path)
            if not s3_key:
                raise FileNotFoundException("Could not find file at %s" % self.path)

            fileobj = ReadableS3File(s3_key)
            return self.format.pipe_reader(fileobj)
        else:
            return self.format.pipe_writer(AtomicS3File(self.path, self.fs))


class S3FlagTarget(S3Target):
    """
    Defines a target directory with a flag-file (defaults to `_SUCCESS`) used
    to signify job success.

    This checks for two things:

    * the path exists (just like the S3Target)
    * the _SUCCESS file exists within the directory.

    Because Hadoop outputs into a directory and not a single file,
    the path is assumed to be a directory.

    This is meant to be a handy alternative to AtomicS3File.

    The AtomicFile approach can be burdensome for S3 since there are no directories, per se.

    If we have 1,000,000 output files, then we have to rename 1,000,000 objects.
    """

    fs = None

    def __init__(self, path, format=None, client=None, flag='_SUCCESS'):
        """
        Initializes a S3FlagTarget.

        :param path: the directory where the files are stored.
        :type path: str
        :param client:
        :type client:
        :param flag:
        :type flag: str
        """
        if format is None:
            format = get_default_format()

        if path[-1] != "/":
            raise ValueError("S3FlagTarget requires the path to be to a "
                             "directory.  It must end with a slash ( / ).")
        super(S3FlagTarget, self).__init__(path)
        self.format = format
        self.fs = client or S3Client()
        self.flag = flag

    def exists(self):
        hadoopSemaphore = self.path + self.flag
        return self.fs.exists(hadoopSemaphore)


class S3EmrTarget(S3FlagTarget):
    """
    Deprecated. Use :py:class:`S3FlagTarget`
    """

    def __init__(self, *args, **kwargs):
        warnings.warn("S3EmrTarget is deprecated. Please use S3FlagTarget")
        super(S3EmrTarget, self).__init__(*args, **kwargs)


class S3PathTask(ExternalTask):
    """
    A external task that to require existence of a path in S3.
    """
    path = Parameter()

    def output(self):
        return S3Target(self.path)


class S3EmrTask(ExternalTask):
    """
    An external task that requires the existence of EMR output in S3.
    """
    path = Parameter()

    def output(self):
        return S3EmrTarget(self.path)


class S3FlagTask(ExternalTask):
    """
    An external task that requires the existence of EMR output in S3.
    """
    path = Parameter()
    flag = Parameter(default=None)

    def output(self):
        return S3FlagTarget(self.path, flag=self.flag)
