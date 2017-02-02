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
:py:class:`S3Target` is a subclass of the Target class to support S3 file
system operations. The `boto` library is required to use S3 targets.
"""

from __future__ import division

import datetime
import itertools
import logging
import os
import os.path

import time
from multiprocessing.pool import ThreadPool

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
from luigi.format import get_default_format
from luigi.parameter import Parameter
from luigi.target import FileAlreadyExists, FileSystem, FileSystemException, FileSystemTarget, AtomicLocalFile, MissingParentDirectory
from luigi.task import ExternalTask

logger = logging.getLogger('luigi-interface')


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
        # only import boto when needed to allow top-lvl s3 module import
        import boto
        import boto.s3.connection
        from boto.s3.key import Key

        options = self._get_s3_config()
        options.update(kwargs)
        # Removing key args would break backwards compability
        role_arn = options.get('aws_role_arn')
        role_session_name = options.get('aws_role_session_name')

        aws_session_token = None

        if role_arn and role_session_name:
            from boto import sts

            sts_client = sts.STSConnection()
            assumed_role = sts_client.assume_role(role_arn, role_session_name)
            aws_secret_access_key = assumed_role.credentials.secret_key
            aws_access_key_id = assumed_role.credentials.access_key
            aws_session_token = assumed_role.credentials.session_token

        else:
            if not aws_access_key_id:
                aws_access_key_id = options.get('aws_access_key_id')

            if not aws_secret_access_key:
                aws_secret_access_key = options.get('aws_secret_access_key')

        for key in ['aws_access_key_id', 'aws_secret_access_key', 'aws_role_session_name', 'aws_role_arn']:
            if key in options:
                options.pop(key)

        self.s3 = boto.s3.connection.S3Connection(aws_access_key_id,
                                                  aws_secret_access_key,
                                                  security_token=aws_session_token,
                                                  **options)
        self.Key = Key

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

        if self.isdir(path):
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

        if self.isdir(path) and not recursive:
            raise InvalidDeleteException('Path %s is a directory. Must use recursive delete' % path)

        delete_key_list = [
            k for k in s3_bucket.list(self._add_path_delimiter(key))]

        # delete the directory marker file if it exists
        s3_dir_with_suffix_key = s3_bucket.get_key(key + S3_DIRECTORY_MARKER_SUFFIX_0)
        if s3_dir_with_suffix_key:
            delete_key_list.append(s3_dir_with_suffix_key)

        if len(delete_key_list) > 0:
            for k in delete_key_list:
                logger.debug('Deleting %s from bucket %s', k, bucket)
            s3_bucket.delete_keys(delete_key_list)
            return True

        return False

    def get_key(self, path):
        """
        Returns just the key from the path.

        An s3 path is composed of a bucket and a key.

        Suppose we have a path `s3://my_bucket/some/files/my_file`. The key is `some/files/my_file`.
        """
        (bucket, key) = self._path_to_bucket_and_key(path)

        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        return s3_bucket.get_key(key)

    def put(self, local_path, destination_s3_path, **kwargs):
        """
        Put an object stored locally to an S3 path.

        :param kwargs: Keyword arguments are passed to the boto function `set_contents_from_filename`
        """
        (bucket, key) = self._path_to_bucket_and_key(destination_s3_path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # put the file
        s3_key = self.Key(s3_bucket)
        s3_key.key = key
        s3_key.set_contents_from_filename(local_path, **kwargs)

    def put_string(self, content, destination_s3_path, **kwargs):
        """
        Put a string to an S3 path.

        :param kwargs: Keyword arguments are passed to the boto function `set_contents_from_string`
        """
        (bucket, key) = self._path_to_bucket_and_key(destination_s3_path)
        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # put the content
        s3_key = self.Key(s3_bucket)
        s3_key.key = key
        s3_key.set_contents_from_string(content, **kwargs)

    def put_multipart(self, local_path, destination_s3_path, part_size=67108864, **kwargs):
        """
        Put an object stored locally to an S3 path
        using S3 multi-part upload (for files > 5GB).

        :param local_path: Path to source local file
        :param destination_s3_path: URL for target S3 location
        :param part_size: Part size in bytes. Default: 67108864 (64MB), must be >= 5MB and <= 5 GB.
        :param kwargs: Keyword arguments are passed to the boto function `initiate_multipart_upload`
        """
        # calculate number of parts to upload
        # based on the size of the file
        source_size = os.stat(local_path).st_size

        if source_size <= part_size:
            # fallback to standard, non-multipart strategy
            return self.put(local_path, destination_s3_path, **kwargs)

        (bucket, key) = self._path_to_bucket_and_key(destination_s3_path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # calculate the number of parts (int division).
        # use modulo to avoid float precision issues
        # for exactly-sized fits
        num_parts = (source_size + part_size - 1) // part_size

        mp = None
        try:
            mp = s3_bucket.initiate_multipart_upload(key, **kwargs)

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

    def get(self, s3_path, destination_local_path):
        """
        Get an object stored in S3 and write it to a local path.
        """
        (bucket, key) = self._path_to_bucket_and_key(s3_path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # download the file
        s3_key = self.Key(s3_bucket)
        s3_key.key = key
        s3_key.get_contents_to_filename(destination_local_path)

    def get_as_string(self, s3_path):
        """
        Get the contents of an object stored in S3 as a string.
        """
        (bucket, key) = self._path_to_bucket_and_key(s3_path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        # get the content
        s3_key = self.Key(s3_bucket)
        s3_key.key = key
        contents = s3_key.get_contents_as_string()

        return contents

    def copy(self, source_path, destination_path, threads=100, start_time=None, end_time=None, part_size=67108864, **kwargs):
        """
        Copy object(s) from one S3 location to another. Works for individual keys or entire directories.

        When files are larger than `part_size`, multipart uploading will be used.

        :param source_path: The `s3://` path of the directory or key to copy from
        :param destination_path: The `s3://` path of the directory or key to copy to
        :param threads: Optional argument to define the number of threads to use when copying (min: 3 threads)
        :param start_time: Optional argument to copy files with modified dates after start_time
        :param end_time: Optional argument to copy files with modified dates before end_time
        :param part_size: Part size in bytes. Default: 67108864 (64MB), must be >= 5MB and <= 5 GB.
        :param kwargs: Keyword arguments are passed to the boto function `copy_key`

        :returns tuple (number_of_files_copied, total_size_copied_in_bytes)
        """
        start = datetime.datetime.now()

        (src_bucket, src_key) = self._path_to_bucket_and_key(source_path)
        (dst_bucket, dst_key) = self._path_to_bucket_and_key(destination_path)

        # As the S3 copy command is completely server side, there is no issue with issuing a lot of threads
        # to issue a single API call per copy, however, this may in theory cause issues on systems with low ulimits for
        # number of threads when copying really large files (e.g. with a ~100GB file this will open ~1500
        # threads), or large directories. Around 100 threads seems to work well.

        threads = 3 if threads < 3 else threads  # don't allow threads to be less than 3
        total_keys = 0

        copy_pool = ThreadPool(processes=threads)

        if self.isdir(source_path):
            # The management pool is to ensure that there's no deadlock between the s3 copying threads, and the
            # multipart_copy threads that monitors each group of s3 copy threads and returns a success once the entire file
            # is copied. Without this, we could potentially fill up the pool with threads waiting to check if the s3 copies
            # have completed, leaving no available threads to actually perform any copying.
            copy_jobs = []
            management_pool = ThreadPool(processes=threads)

            (bucket, key) = self._path_to_bucket_and_key(source_path)
            key_path = self._add_path_delimiter(key)
            key_path_len = len(key_path)

            total_size_bytes = 0
            src_prefix = self._add_path_delimiter(src_key)
            dst_prefix = self._add_path_delimiter(dst_key)
            for item in self.list(source_path, start_time=start_time, end_time=end_time, return_key=True):
                path = item.key[key_path_len:]
                # prevents copy attempt of empty key in folder
                if path != '' and path != '/':
                    total_keys += 1
                    total_size_bytes += item.size
                    job = management_pool.apply_async(self.__copy_multipart,
                                                      args=(copy_pool,
                                                            src_bucket, src_prefix + path,
                                                            dst_bucket, dst_prefix + path,
                                                            part_size),
                                                      kwds=kwargs)
                    copy_jobs.append(job)

            # Wait for the pools to finish scheduling all the copies
            management_pool.close()
            management_pool.join()
            copy_pool.close()
            copy_pool.join()

            # Raise any errors encountered in any of the copy processes
            for result in copy_jobs:
                result.get()

            end = datetime.datetime.now()
            duration = end - start
            logger.info('%s : Complete : %s total keys copied in %s' %
                        (datetime.datetime.now(), total_keys, duration))

            return total_keys, total_size_bytes

        # If the file isn't a directory just perform a simple copy
        else:
            self.__copy_multipart(copy_pool, src_bucket, src_key, dst_bucket, dst_key, part_size, **kwargs)
            # Close the pool
            copy_pool.close()
            copy_pool.join()

    def __copy_multipart(self, pool, src_bucket, src_key, dst_bucket, dst_key, part_size, **kwargs):
        """
        Copy a single S3 object to another S3 object, falling back to multipart copy where necessary

        NOTE: This is a private method and should only be called from within the `luigi.s3.copy` method

        :param pool: The threadpool to put the s3 copy processes onto
        :param src_bucket: source bucket name
        :param src_key: source key name
        :param dst_bucket: destination bucket name
        :param dst_key: destination key name
        :param key_size: size of the key to copy in bytes
        :param part_size: Part size in bytes. Must be >= 5MB and <= 5 GB.
        :param kwargs: Keyword arguments are passed to the boto function `initiate_multipart_upload`
        """

        source_bucket = self.s3.get_bucket(src_bucket, validate=True)
        dest_bucket = self.s3.get_bucket(dst_bucket, validate=True)

        key_size = source_bucket.lookup(src_key).size

        # We can't do a multipart copy on an empty Key, so handle this specially.
        # Also, don't bother using the multipart machinery if we're only dealing with a small non-multipart file
        if key_size == 0 or key_size <= part_size:
            result = pool.apply_async(dest_bucket.copy_key, args=(dst_key, src_bucket, src_key), kwds=kwargs)
            # Bubble up any errors we may encounter
            return result.get()

        mp = None

        try:
            mp = dest_bucket.initiate_multipart_upload(dst_key, **kwargs)
            cur_pos = 0

            # Store the results from the apply_async in a list so we can check for failures
            results = []

            # Calculate the number of chunks the file will be
            num_parts = (key_size + part_size - 1) // part_size

            for i in range(num_parts):
                # Issue an S3 copy request, one part at a time, from one S3 object to another
                part_start = cur_pos
                cur_pos += part_size
                part_end = min(cur_pos - 1, key_size - 1)
                part_num = i + 1
                results.append(pool.apply_async(mp.copy_part_from_key, args=(src_bucket, src_key, part_num, part_start, part_end)))
                logger.info('Requesting copy of %s/%s to %s/%s', part_num, num_parts, dst_bucket, dst_key)

            logger.info('Waiting for multipart copy of %s/%s to finish', dst_bucket, dst_key)

            # This will raise any exceptions in any of the copy threads
            for result in results:
                result.get()

            # finish the copy, making the file available in S3
            mp.complete_upload()
            return mp.key_name

        except:
            logger.info('Error during multipart s3 copy for %s/%s to %s/%s...', src_bucket, src_key, dst_bucket, dst_key)
            # cancel the copy so we don't get charged for storage consumed by copied parts
            if mp:
                mp.cancel_upload()
            raise

    def move(self, source_path, destination_path, **kwargs):
        """
        Rename/move an object from one S3 location to another.

        :param kwargs: Keyword arguments are passed to the boto function `copy_key`
        """
        self.copy(source_path, destination_path, **kwargs)
        self.remove(source_path)

    def listdir(self, path, start_time=None, end_time=None, return_key=False):
        """
        Get an iterable with S3 folder contents.
        Iterable contains paths relative to queried path.

        :param start_time: Optional argument to list files with modified dates after start_time
        :param end_time: Optional argument to list files with modified dates before end_time
        :param return_key: Optional argument, when set to True will return a boto.s3.key.Key (instead of the filename)
        """
        (bucket, key) = self._path_to_bucket_and_key(path)

        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)

        key_path = self._add_path_delimiter(key)
        key_path_len = len(key_path)
        for item in s3_bucket.list(prefix=key_path):
            last_modified_date = time.strptime(item.last_modified, "%Y-%m-%dT%H:%M:%S.%fZ")
            if (
                    (not start_time and not end_time) or  # neither are defined, list all
                    (start_time and not end_time and start_time < last_modified_date) or  # start defined, after start
                    (not start_time and end_time and last_modified_date < end_time) or  # end defined, prior to end
                    (start_time and end_time and start_time < last_modified_date < end_time)  # both defined, between
               ):
                if return_key:
                    yield item
                else:
                    yield self._add_path_delimiter(path) + item.key[key_path_len:]

    def list(self, path, start_time=None, end_time=None, return_key=False):  # backwards compat
        key_path_len = len(self._add_path_delimiter(path))
        for item in self.listdir(path, start_time=start_time, end_time=end_time, return_key=return_key):
            if return_key:
                yield item
            else:
                yield item[key_path_len:]

    def isdir(self, path):
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
        s3_bucket_list_result = list(itertools.islice(s3_bucket.list(prefix=key_path), 1))
        if s3_bucket_list_result:
            return True

        return False

    is_dir = isdir  # compatibility with old version.

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if raise_if_exists and self.isdir(path):
            raise FileAlreadyExists()

        _, key = self._path_to_bucket_and_key(path)
        if self._is_root(key):
            return  # isdir raises if the bucket doesn't exist; nothing to do here.

        key = self._add_path_delimiter(key)

        if not parents and not self.isdir(os.path.dirname(key)):
            raise MissingParentDirectory()

        return self.put_string("", self._add_path_delimiter(path))

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
        return key if key[-1:] == '/' or key == '' else key + '/'


class AtomicS3File(AtomicLocalFile):
    """
    An S3 file that writes to a temp file and puts to S3 on close.

    :param kwargs: Keyword arguments are passed to the boto function `initiate_multipart_upload`
    """

    def __init__(self, path, s3_client, **kwargs):
        self.s3_client = s3_client
        super(AtomicS3File, self).__init__(path)
        self.s3_options = kwargs

    def move_to_final_destination(self):
        self.s3_client.put_multipart(self.tmp_path, self.path, **self.s3_options)


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
    Target S3 file object

    :param kwargs: Keyword arguments are passed to the boto function `initiate_multipart_upload`
    """

    fs = None

    def __init__(self, path, format=None, client=None, **kwargs):
        super(S3Target, self).__init__(path)
        if format is None:
            format = get_default_format()

        self.path = path
        self.format = format
        self.fs = client or S3Client()
        self.s3_options = kwargs

    def open(self, mode='r'):
        if mode not in ('r', 'w'):
            raise ValueError("Unsupported open mode '%s'" % mode)

        if mode == 'r':
            s3_key = self.fs.get_key(self.path)
            if not s3_key:
                raise FileNotFoundException("Could not find file at %s" % self.path)

            fileobj = ReadableS3File(s3_key)
            return self.format.pipe_reader(fileobj)
        else:
            return self.format.pipe_writer(AtomicS3File(self.path, self.fs, **self.s3_options))


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
        super(S3FlagTarget, self).__init__(path, format, client)
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
