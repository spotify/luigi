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

import signal
import subprocess
import io
import os
import re
import locale
import tempfile
import warnings

from luigi import six


class FileWrapper(object):
    """
    Wrap `file` in a "real" so stuff can be added to it after creation.
    """

    def __init__(self, file_object):
        self._subpipe = file_object

    def __getattr__(self, name):
        # forward calls to 'write', 'close' and other methods not defined below
        return getattr(self._subpipe, name)

    def __enter__(self, *args, **kwargs):
        # instead of returning whatever is returned by __enter__ on the subpipe
        # this returns self, so whatever custom injected methods are still available
        # this might cause problems with custom file_objects, but seems to work
        # fine with standard python `file` objects which is the only default use
        return self

    def __exit__(self, *args, **kwargs):
        return self._subpipe.__exit__(*args, **kwargs)

    def __iter__(self):
        return iter(self._subpipe)


class InputPipeProcessWrapper(object):

    def __init__(self, command, input_pipe=None):
        """
        Initializes a InputPipeProcessWrapper instance.

        :param command: a subprocess.Popen instance with stdin=input_pipe and
                        stdout=subprocess.PIPE.
                        Alternatively, just its args argument as a convenience.
        """
        self._command = command

        self._input_pipe = input_pipe
        self._original_input = True

        if input_pipe is not None:
            try:
                input_pipe.fileno()
            except AttributeError:
                # subprocess require a fileno to work, if not present we copy to disk first
                self._original_input = False
                f = tempfile.NamedTemporaryFile('wb', prefix='luigi-process_tmp', delete=False)
                self._tmp_file = f.name
                while True:
                    chunk = input_pipe.read(io.DEFAULT_BUFFER_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                input_pipe.close()
                f.close()
                self._input_pipe = FileWrapper(io.BufferedReader(io.FileIO(self._tmp_file, 'r')))

        self._process = command if isinstance(command, subprocess.Popen) else self.create_subprocess(command)
        # we want to keep a circular reference to avoid garbage collection
        # when the object is used in, e.g., pipe.read()
        self._process._selfref = self

    def create_subprocess(self, command):
        """
        http://www.chiark.greenend.org.uk/ucgi/~cjwatson/blosxom/2009-07-02-python-sigpipe.html
        """

        def subprocess_setup():
            # Python installs a SIGPIPE handler by default. This is usually not what
            # non-Python subprocesses expect.
            signal.signal(signal.SIGPIPE, signal.SIG_DFL)

        return subprocess.Popen(command,
                                stdin=self._input_pipe,
                                stdout=subprocess.PIPE,
                                preexec_fn=subprocess_setup,
                                close_fds=True)

    def _finish(self):
        # Need to close this before input_pipe to get all SIGPIPE messages correctly
        self._process.stdout.close()
        if not self._original_input and os.path.exists(self._tmp_file):
            os.remove(self._tmp_file)

        if self._input_pipe is not None:
            self._input_pipe.close()

        self._process.wait()  # deadlock?
        if self._process.returncode not in (0, 141, 128 - 141):
            # 141 == 128 + 13 == 128 + SIGPIPE - normally processes exit with 128 + {reiceived SIG}
            # 128 - 141 == -13 == -SIGPIPE, sometimes python receives -13 for some subprocesses
            raise RuntimeError('Error reading from pipe. Subcommand exited with non-zero exit status %s.' % self._process.returncode)

    def close(self):
        self._finish()

    def __del__(self):
        self._finish()

    def __enter__(self):
        return self

    def _abort(self):
        """
        Call _finish, but eat the exception (if any).
        """
        try:
            self._finish()
        except KeyboardInterrupt:
            raise
        except BaseException:
            pass

    def __exit__(self, type, value, traceback):
        if type:
            self._abort()
        else:
            self._finish()

    def __getattr__(self, name):
        if name in ['_process', '_input_pipe']:
            raise AttributeError(name)
        try:
            return getattr(self._process.stdout, name)
        except AttributeError:
            return getattr(self._input_pipe, name)

    def __iter__(self):
        for line in self._process.stdout:
            yield line
        self._finish()

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return False


class OutputPipeProcessWrapper(object):
    WRITES_BEFORE_FLUSH = 10000

    def __init__(self, command, output_pipe=None):
        self.closed = False
        self._command = command
        self._output_pipe = output_pipe
        self._process = subprocess.Popen(command,
                                         stdin=subprocess.PIPE,
                                         stdout=output_pipe,
                                         close_fds=True)
        self._flushcount = 0

    def write(self, *args, **kwargs):
        self._process.stdin.write(*args, **kwargs)
        self._flushcount += 1
        if self._flushcount == self.WRITES_BEFORE_FLUSH:
            self._process.stdin.flush()
            self._flushcount = 0

    def writeLine(self, line):
        assert '\n' not in line
        self.write(line + '\n')

    def _finish(self):
        """
        Closes and waits for subprocess to exit.
        """
        if self._process.returncode is None:
            self._process.stdin.flush()
            self._process.stdin.close()
            self._process.wait()
            self.closed = True

    def __del__(self):
        if not self.closed:
            self.abort()

    def __exit__(self, type, value, traceback):
        if type is None:
            self.close()
        else:
            self.abort()

    def __enter__(self):
        return self

    def close(self):
        self._finish()
        if self._process.returncode == 0:
            if self._output_pipe is not None:
                self._output_pipe.close()
        else:
            raise RuntimeError('Error when executing command %s' % self._command)

    def abort(self):
        self._finish()

    def __getattr__(self, name):
        if name in ['_process', '_output_pipe']:
            raise AttributeError(name)
        try:
            return getattr(self._process.stdin, name)
        except AttributeError:
            return getattr(self._output_pipe, name)

    def readable(self):
        return False

    def writable(self):
        return True

    def seekable(self):
        return False


class BaseWrapper(object):

    def __init__(self, stream, *args, **kwargs):
        self._stream = stream
        try:
            super(BaseWrapper, self).__init__(stream, *args, **kwargs)
        except TypeError:
            pass

    def __getattr__(self, name):
        if name == '_stream':
            raise AttributeError(name)
        return getattr(self._stream, name)

    def __enter__(self):
        self._stream.__enter__()
        return self

    def __exit__(self, *args):
        self._stream.__exit__(*args)

    def __iter__(self):
        try:
            for line in self._stream:
                yield line
        finally:
            self.close()


class NewlineWrapper(BaseWrapper):

    def __init__(self, stream, newline=None):
        if newline is None:
            self.newline = newline
        else:
            self.newline = newline.encode('ascii')

        if self.newline not in (b'', b'\r\n', b'\n', b'\r', None):
            raise ValueError("newline need to be one of {b'', b'\r\n', b'\n', b'\r', None}")
        super(NewlineWrapper, self).__init__(stream)

    def read(self, n=-1):
        b = self._stream.read(n)

        if self.newline == b'':
            return b

        if self.newline is None:
            newline = b'\n'

        return re.sub(b'(\n|\r\n|\r)', newline, b)

    def writelines(self, lines):
        if self.newline is None or self.newline == '':
            newline = os.linesep.encode('ascii')
        else:
            newline = self.newline

        self._stream.writelines(
            (re.sub(b'(\n|\r\n|\r)', newline, line) for line in lines)
        )

    def write(self, b):
        if self.newline is None or self.newline == '':
            newline = os.linesep.encode('ascii')
        else:
            newline = self.newline

        self._stream.write(re.sub(b'(\n|\r\n|\r)', newline, b))


class MixedUnicodeBytesWrapper(BaseWrapper):
    """
    """

    def __init__(self, stream, encoding=None):
        if encoding is None:
            encoding = locale.getpreferredencoding()
        self.encoding = encoding
        super(MixedUnicodeBytesWrapper, self).__init__(stream)

    def write(self, b):
        self._stream.write(self._convert(b))

    def writelines(self, lines):
        self._stream.writelines((self._convert(line) for line in lines))

    def _convert(self, b):
        if isinstance(b, six.text_type):
            b = b.encode(self.encoding)
            warnings.warn('Writing unicode to byte stream', stacklevel=2)
        return b


class Format(object):
    """
    Interface for format specifications.
    """

    @classmethod
    def pipe_reader(cls, input_pipe):
        raise NotImplementedError()

    @classmethod
    def pipe_writer(cls, output_pipe):
        raise NotImplementedError()

    def __rshift__(a, b):
        return ChainFormat(a, b)


class ChainFormat(Format):

    def __init__(self, *args, **kwargs):
        self.args = args
        try:
            self.input = args[0].input
        except AttributeError:
            pass
        try:
            self.output = args[-1].output
        except AttributeError:
            pass
        if not kwargs.get('check_consistency', True):
            return
        for x in range(len(args) - 1):
            try:
                if args[x].output != args[x + 1].input:
                    raise TypeError(
                        'The format chaining is not valid, %s expect %s'
                        ' but %s provide %s' % (
                            args[x + 1].__class__.__name__,
                            args[x + 1].input,
                            args[x].__class__.__name__,
                            args[x].output,
                        )
                    )
            except AttributeError:
                pass

    def pipe_reader(self, input_pipe):
        for x in reversed(self.args):
            input_pipe = x.pipe_reader(input_pipe)
        return input_pipe

    def pipe_writer(self, output_pipe):
        for x in reversed(self.args):
            output_pipe = x.pipe_writer(output_pipe)
        return output_pipe


class TextWrapper(io.TextIOWrapper):

    def __exit__(self, *args):
        # io.TextIOWrapper close the file on __exit__, let the underlying file decide
        if not self.closed and self.writable():
            super(TextWrapper, self).flush()

        self._stream.__exit__(*args)

    def __del__(self, *args):
        # io.TextIOWrapper close the file on __del__, let the underlying file decide
        if not self.closed and self.writable():
            super(TextWrapper, self).flush()

        try:
            self._stream.__del__(*args)
        except AttributeError:
            pass

    def __init__(self, stream, *args, **kwargs):
        self._stream = stream
        try:
            super(TextWrapper, self).__init__(stream, *args, **kwargs)
        except TypeError:
            pass

    def __getattr__(self, name):
        if name == '_stream':
            raise AttributeError(name)
        return getattr(self._stream, name)

    def __enter__(self):
        self._stream.__enter__()
        return self


class NopFormat(Format):
    def pipe_reader(self, input_pipe):
        return input_pipe

    def pipe_writer(self, output_pipe):
        return output_pipe


class WrappedFormat(Format):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def pipe_reader(self, input_pipe):
        return self.wrapper_cls(input_pipe, *self.args, **self.kwargs)

    def pipe_writer(self, output_pipe):
        return self.wrapper_cls(output_pipe, *self.args, **self.kwargs)


class TextFormat(WrappedFormat):

    input = 'unicode'
    output = 'bytes'
    wrapper_cls = TextWrapper


class MixedUnicodeBytesFormat(WrappedFormat):

    output = 'bytes'
    wrapper_cls = MixedUnicodeBytesWrapper


class NewlineFormat(WrappedFormat):

    input = 'bytes'
    output = 'bytes'
    wrapper_cls = NewlineWrapper


class GzipFormat(Format):

    input = 'bytes'
    output = 'bytes'

    def __init__(self, compression_level=None):
        self.compression_level = compression_level

    def pipe_reader(self, input_pipe):
        return InputPipeProcessWrapper(['gunzip'], input_pipe)

    def pipe_writer(self, output_pipe):
        args = ['gzip']
        if self.compression_level is not None:
            args.append('-' + str(int(self.compression_level)))
        return OutputPipeProcessWrapper(args, output_pipe)


class Bzip2Format(Format):

    input = 'bytes'
    output = 'bytes'

    def pipe_reader(self, input_pipe):
        return InputPipeProcessWrapper(['bzcat'], input_pipe)

    def pipe_writer(self, output_pipe):
        return OutputPipeProcessWrapper(['bzip2'], output_pipe)


Text = TextFormat()
UTF8 = TextFormat(encoding='utf8')
Nop = NopFormat()
SysNewLine = NewlineFormat()
Gzip = GzipFormat()
Bzip2 = Bzip2Format()
MixedUnicodeBytes = MixedUnicodeBytesFormat()


def get_default_format():
    if six.PY3:
        return Text
    elif os.linesep == '\n':
        return Nop
    else:
        return SysNewLine
