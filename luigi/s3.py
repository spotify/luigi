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

import sys
import threading
import time
from multiprocessing.pool import ThreadPool
from threading import Thread, BoundedSemaphore

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
        from boto.s3.key import Key

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

    def copy(self, source_path, destination_path, threads=3, start_time=None, end_time=None, **kwargs):
        """
        Copy an object from one S3 location to another.

        :param threads: Optional argument to define the number of threads to use when copying (min: 3 threads)
        :param start_time: Optional argument to copy files with modified dates after start_time
        :param end_time: Optional argument to copy files with modified dates before end_time
        :param kwargs: Keyword arguments are passed to the boto function `copy_key`
        """
        start = datetime.datetime.now()

        (src_bucket, src_key) = self._path_to_bucket_and_key(source_path)
        (dst_bucket, dst_key) = self._path_to_bucket_and_key(destination_path)
        src_prefix = self._add_path_delimiter(src_key)
        dst_prefix = self._add_path_delimiter(dst_key)

        s3_bucket = self.s3.get_bucket(dst_bucket, validate=True)
        source_bucket = self.s3.get_bucket(src_bucket, validate=True)

        # If the file is larger than 64MB, then use multipart copy to perform the
        # copy faster. This constant was chosen in the the original put_multipart
        # implementation, and I've just used it here.
        multipart_threshold = 67108864

        key_copy_thread_list = []
        threads = 3 if threads < 3 else threads  # don't allow threads to be less than 3
        pool_semaphore = BoundedSemaphore(value=threads)
        total_keys = 0

        class CopyKey(Thread):
            def __init__(self, s3, key):
                Thread.__init__(self)
                self.s3 = s3
                self.key = key
                self.status = None

            def run(self):
                thread_dst_bucket = self.s3.get_bucket(dst_bucket, validate=True)

                pool_semaphore.acquire()
                self.status = '%s : Semaphore Acquired, Copy Next' % datetime.datetime.now()
                try:
                    thread_dst_bucket.copy_key(dst_prefix + self.key,
                                               src_bucket,
                                               src_prefix + self.key, **kwargs)
                    self.status = '%s : Copy Success : %s' % (datetime.datetime.now(), self.key)
                except:
                    self.status = '%s : Copy Error : %s' % (datetime.datetime.now(), sys.exc_info())
                finally:
                    pool_semaphore.release()

        if self.isdir(source_path):
            max_thread_count = 0
            for key in self.list(source_path, start_time=start_time, end_time=end_time):
                if source_bucket.lookup(src_prefix + key).size <= multipart_threshold:
                    if key != '' and key != '/':  # prevents copy attempt of empty key in folder
                        total_keys += 1

                        current = CopyKey(self.s3, key)
                        key_copy_thread_list.append(current)
                        current.start()  # start new thread

                        if len(threading.enumerate()) > max_thread_count:
                            max_thread_count = len(threading.enumerate())

                        # Pause if max threads reached
                        # note that enumerate returns all threads, including this parent thread
                        if len(threading.enumerate()) >= threads:
                            while True:
                                if len(threading.enumerate()) < threads:
                                    break  # continues to create threads
                                time.sleep(1)
                else:
                    self.copy_multipart(dst_prefix + key,
                                        src_bucket,
                                        src_prefix + key, **kwargs)

            for key_copy_thread in key_copy_thread_list:
                # Bring this thread to current "parent" thread, blocks parent until joined or 30s timeout
                key_copy_thread.join(30)
                if key_copy_thread.isAlive():
                    logger.debug('%s : TIMEOUT on key %s' % (datetime.datetime.now(), key_copy_thread.key_name))
                    continue

            end = datetime.datetime.now()
            duration = end - start
            logger.info('%s : Complete : %s Total Keys Requested in %s' %
                        (datetime.datetime.now(), total_keys, duration))
            logger.debug('Max Num Active Threads: %d' % max_thread_count)

        elif source_bucket.lookup(src_key).size <= multipart_threshold:
            s3_bucket.copy_key(dst_key, src_bucket, src_key, **kwargs)
        else:
            self.copy_multipart(source_path, destination_path, **kwargs)

    def copy_multipart(self, source_path, destination_path, part_size=67108864, **kwargs):
        """
        Copy a single S3 object to another S3 object using S3 multi-part copy (for files > 5GB).
        It will use a single thread per request so that all parts are requested to be moved simultaneously
        for maximum speed.

        :param source_path: URL for S3 Source
        :param destination_path: URL for target S3 location
        :param part_size: Part size in bytes. Default: 67108864 (64MB), must be >= 5MB and <= 5 GB.
        :param kwargs: Keyword arguments are passed to the boto function `initiate_multipart_upload`
        """
        (src_bucket, src_key) = self._path_to_bucket_and_key(source_path)
        (dst_bucket, dst_key) = self._path_to_bucket_and_key(destination_path)

        dest_bucket = self.s3.get_bucket(dst_bucket, validate=True)
        source_bucket = self.s3.get_bucket(src_bucket, validate=True)

        source_size = source_bucket.lookup(src_key).size

        num_parts = (source_size + part_size - 1) // part_size

        # As the S3 copy command is completely server side, there is no issue with issuing a single
        # API call per part, however, this may in theory cause issues on systems with low ulimits for
        # number of threads when copying really large files, e.g. with a ~100GB file this will open ~1500
        # threads. We take the max of this and 1 as if we're copying an empty file we will have  `num_part == 0`
        pool = ThreadPool(processes=max(1, num_parts))

        mp = None
        try:
            mp = dest_bucket.initiate_multipart_upload(dst_key, **kwargs)
            cur_pos = 0

            # Store the results from the apply_async in a list so we can check for failures
            results = []

            for i in range(num_parts):
                # Issue an S3 copy request, one part at a time, from one S3 object to another
                part_start = cur_pos
                cur_pos += part_size
                part_end = min(cur_pos - 1, source_size - 1)
                part_num = i + 1
                results.append(pool.apply_async(mp.copy_part_from_key, args=(src_bucket, src_key, part_num, part_start, part_end)))
                logger.info('Requesting copy of %s/%s to %s', part_num, num_parts, destination_path)

            logger.info('Waiting for multipart copy of %s to finish', destination_path)
            pool.close()
            pool.join()

            # This will raise any exceptions in any of the copy threads
            for result in results:
                result.get()

            # finish the copy, making the file available in S3
            mp.complete_upload()
        except BaseException:
            if mp:
                logger.info('Canceling multipart s3 copy for %s to %s', source_path, destination_path)
                # cancel the copy so we don't get charged for
                # storage consumed by copied parts
                mp.cancel_upload()
            raise

    def rename(self, *args, **kwargs):
        """
        Alias for ``move()``
        """
        self.move(*args, **kwargs)

    def move(self, source_path, destination_path, **kwargs):
        """
        Rename/move an object from one S3 location to another.

        :param kwargs: Keyword arguments are passed to the boto function `copy_key`
        """
        self.copy(source_path, destination_path, **kwargs)
        self.remove(source_path)

    def listdir(self, path, start_time=None, end_time=None):
        """
        Get an iterable with S3 folder contents.
        Iterable contains paths relative to queried path.

        :param start_time: Optional argument to copy files with modified dates after start_time
        :param end_time: Optional argument to copy files with modified dates before end_time
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
                yield self._add_path_delimiter(path) + item.key[key_path_len:]

    def list(self, path, start_time=None, end_time=None):  # backwards compat
        key_path_len = len(self._add_path_delimiter(path))
        for item in self.listdir(path, start_time=start_time, end_time=end_time):
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
    An S3 file that writes to a temp file and put to S3 on close.

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
