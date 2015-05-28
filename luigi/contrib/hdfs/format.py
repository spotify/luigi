import luigi.format
import logging
import os
from luigi.contrib.hdfs.config import load_hadoop_cmd
from luigi.contrib.hdfs import config as hdfs_config
from luigi.contrib.hdfs.clients import remove, rename, mkdir

logger = logging.getLogger('luigi-interface')


class HdfsReadPipe(luigi.format.InputPipeProcessWrapper):

    def __init__(self, path):
        super(HdfsReadPipe, self).__init__(load_hadoop_cmd() + ['fs', '-cat', path])


class HdfsAtomicWritePipe(luigi.format.OutputPipeProcessWrapper):
    """
    File like object for writing to HDFS

    The referenced file is first written to a temporary location and then
    renamed to final location on close(). If close() isn't called
    the temporary file will be cleaned up when this object is
    garbage collected

    TODO: if this is buggy, change it so it first writes to a
    local temporary file and then uploads it on completion

    If the append keyword arugment is set to True it will appendToFile instead
    of put.
    """

    def __init__(self, path, append=False):
        self.path = path
        self.tmppath = hdfs_config.tmppath(self.path)
        self.append = append
        parent_dir = self.path if append else self.tmppath
        parent_dir = os.path.dirname(parent_dir)
        mkdir(parent_dir, parents=True, raise_if_exists=False)
        command = [
            'fs',
            "-appendToFile" if append else "-put", '-',
            self.path if append else self.tmppath
        ]
        super(HdfsAtomicWritePipe, self).__init__(load_hadoop_cmd() + command)

    def abort(self):
        logger.info("Aborting %s('%s').", self.__class__.__name__)
        super(HdfsAtomicWritePipe, self).abort()
        if not self.append:
            logger.info("Removing temporary file '%s'",
                        self.path, self.tmppath)
            remove(self.tmppath)

    def close(self):
        super(HdfsAtomicWritePipe, self).close()
        if not self.append:
            rename(self.tmppath, self.path)


class HdfsAtomicWriteDirPipe(luigi.format.OutputPipeProcessWrapper):
    """
    Writes a data<data_extension> file to a directory at <path>.
    """

    def __init__(self, path, data_extension=""):
        self.path = path
        self.tmppath = hdfs_config.tmppath(self.path)
        self.datapath = self.tmppath + ("/data%s" % data_extension)
        super(HdfsAtomicWriteDirPipe, self).__init__(load_hadoop_cmd() + ['fs', '-put', '-', self.datapath])

    def abort(self):
        logger.info("Aborting %s('%s'). Removing temporary dir '%s'",
                    self.__class__.__name__, self.path, self.tmppath)
        super(HdfsAtomicWriteDirPipe, self).abort()
        remove(self.tmppath)

    def close(self):
        super(HdfsAtomicWriteDirPipe, self).close()
        rename(self.tmppath, self.path)


class PlainFormat(luigi.format.Format):

    input = 'bytes'
    output = 'hdfs'

    def hdfs_writer(self, path):
        return self.pipe_writer(path)

    def hdfs_reader(self, path):
        return self.pipe_reader(path)

    def pipe_reader(self, path):
        return HdfsReadPipe(path)

    def pipe_writer(self, output_pipe):
        return HdfsAtomicWritePipe(output_pipe)


class PlainDirFormat(luigi.format.Format):

    input = 'bytes'
    output = 'hdfs'

    def hdfs_writer(self, path):
        return self.pipe_writer(path)

    def hdfs_reader(self, path):
        return self.pipe_reader(path)

    def pipe_reader(self, path):
        # exclude underscore-prefixedfiles/folders (created by MapReduce)
        return HdfsReadPipe("%s/[^_]*" % path)

    def pipe_writer(self, path):
        return HdfsAtomicWriteDirPipe(path)


Plain = PlainFormat()
PlainDir = PlainDirFormat()


class CompatibleHdfsFormat(luigi.format.Format):

    output = 'hdfs'

    def __init__(self, writer, reader, input=None):
        if input is not None:
            self.input = input

        self.reader = reader
        self.writer = writer

    def pipe_writer(self, output):
        return self.writer(output)

    def pipe_reader(self, input):
        return self.reader(input)

    def pipe_appender(self, output):
        return HdfsAtomicWritePipe(path=output, append=True)

    def hdfs_writer(self, output):
        return self.writer(output)

    def hdfs_reader(self, input):
        return self.reader(input)
