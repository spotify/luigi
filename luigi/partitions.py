# -*- coding: utf-8 -*-
#
# Copyright 2016 Eventbrite Inc
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


def splitting_prefix_chooser(fs_url, prefix='part-'):
    filename = fs_url.split('/')[-1]
    return filename.startswith(prefix)


class PartitionedFilesReader(object):
    """
    Reads a Flag Target of part-* files as if it were a single file.

    Pass this a Target representing a directory full of part-* files,
    such as an S3FlagTarget, and it will allow you to read the part-* files
    underneath that directory one by one as though they were a single file.
    A target creator callable, such as S3Target, is also necessary.
    It will be passed the canonical URL of each part file in turn, and must return a
    Target-like object that can be opened as readable (e.g., open('r')).

    The user should check that the flag target 'exists()' before using
    this class, otherwise the user may end up reading partial part-* files.

    You can additionally pass a function to help it make proper decisions
    about how to handle the start of a file, e.g. if your CSV files have a header
    that you want to ignore for all files except the first one. This function
    will differ based on whether the reader will be reading by bytes or by lines.

    Examples:

    The line-by-line filter function receives each line once. It can choose to
    accept (return), reject (return blank), or modify (return bytes)
    the line. It will not receive this line again.

    def filter_lines_skip_csv_headers_except_first(line, file_num, line_num):
        if line_num == 0 and file_num > 0:
            return ''  # blank lines are not delivered to the reader at all
        return line

    The byte-by-byte filter function receives a byte buffer containing bytes read
    that have not yet been passed to the reader. If the function returns any bytes,
    those bytes will be returned to the reader as though they were what was read from
    the file. If the function returns no bytes, the bytes will be buffered,
    and the next call to this function will contain those same bytes plus
    the bytes from the subsequent file read. These buffered bytes will not be discarded
    even as the PartitionedFilesReader moves on to the next readable partition file.
    In this way, the filter bytes function is guaranteed to see the raw file bytes in
    chunks of whatever size is necessary for it to make its decisions, as if the
    partitioned files were in fact one big file.

    def filter_bytes_skip_csv_headers(byte_buf, file_num, start_byte_idx, num_bytes_from_current_file):
        if start_byte_idx == 0 and file_num > 0:
            if '\n' not in byte_buf:  # partial header
                return b''
            else:  # full header - and possibly more than one, if there were any header-only files.
                byte_buf = byte_buf[-num_bytes_from_current_file:]  # get rid of bytes from previous files
                return '\n'.join(byte_buf.split('\n')[1:])
        return byte_buf

    """

    def __init__(self, fs_target, target_creator, listdir_chooser=splitting_prefix_chooser,
                 filter_line_fcn=None, filter_bytes_fcn=None):
        self.fs_target = fs_target
        self.target_creator = target_creator
        self.listdir_chooser = listdir_chooser
        self.filter_line_fcn = filter_line_fcn
        self.filter_bytes_fcn = filter_bytes_fcn

        self.dir_list_gen = self.fs_target.fs.listdir(self.fs_target.path)
        self.part_file_num = -1
        self.finished = False

        self._get_next_readable()

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False

    def _get_next_part_file(self):
        next_url = None
        while not next_url:
            try:
                # AWS S3 generator is guaranteed to return items in UTF-8 binary order,
                # which should suffice for making sure we get them in part-00000, part-00001, etc. order.
                # If used for targets other than S3, they will need to be investigated separately.
                next_url = next(self.dir_list_gen)
                if not self.listdir_chooser(next_url):
                    next_url = None  # keep iterating
            except StopIteration:
                break
        return next_url

    def _get_next_readable(self):
        next_target_s3_url = self._get_next_part_file()
        if next_target_s3_url:
            self.current_readable = self.target_creator(next_target_s3_url).open('r')
            self.part_file_num += 1
            self.file_line_num = 0
            self.file_byte_idx = 0
        else:
            self.finished = True

    def read(self, size=0):
        """
        Reads byte by byte. Will return non-empty bytes unless there's nothing left to read from any file.
        """
        if self.finished:
            return b''

        ret_bytes = b''
        byte_buf = b''
        num_bytes_read_from_current_file = 0
        # We correctly inform the filter_bytes_fcn, if any, about the file # and index
        # of the first byte in the file that we're currently reading.
        # However, if the filter_bytes_fcn ignored (returned b'') bytes from the previous
        # file, we do not discard those bytes even when moving to the next file.
        while not self.finished and not ret_bytes:
            read_bytes = self.current_readable.read(size)
            if not read_bytes:  # we need to move to the next file
                self._get_next_readable()
                num_bytes_read_from_current_file = 0
                continue

            num_bytes_read_from_current_file += len(read_bytes)
            byte_buf += read_bytes
            if self.filter_bytes_fcn:
                ret_bytes = self.filter_bytes_fcn(byte_buf, self.part_file_num,
                                                  self.file_byte_idx, num_bytes_read_from_current_file)
            else:
                ret_bytes = byte_buf
        self.file_byte_idx += num_bytes_read_from_current_file
        return ret_bytes

    def __iter__(self):
        """
        Read line by line from all files.
        """
        while not self.finished:
            for next_line in self.current_readable:
                if self.filter_line_fcn:
                    next_line = self.filter_line_fcn(next_line, self.part_file_num, self.file_line_num)
                if next_line:
                    yield next_line
                self.file_line_num += 1
            self._get_next_readable()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        """
        Ensures that the object is no longer readable/iterable, even if there was something left to be read.
        """
        self.finished = True
        self.current_readable = None  # close the last readable object
