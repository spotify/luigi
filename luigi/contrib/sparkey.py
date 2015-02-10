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

from __future__ import absolute_import

import luigi


class SparkeyExportTask(luigi.Task):
    """
    A luigi task that writes to a local sparkey log file.

    Subclasses should implement the requires and output methods. The output
    must be a luigi.LocalTarget.

    The resulting sparkey log file will contain one entry for every line in
    the input, mapping from the first value to a tab-separated list of the
    rest of the line.

    To generate a simple key-value index, yield "key", "value" pairs from the input(s) to this task.
    """

    # the separator used to split input lines
    separator = '\t'

    def __init__(self, *args, **kwargs):
        super(SparkeyExportTask, self).__init__(*args, **kwargs)

    def run(self):
        self._write_sparkey_file()

    def _write_sparkey_file(self):
        import sparkey

        infile = self.input()
        outfile = self.output()
        if not isinstance(outfile, luigi.LocalTarget):
            raise TypeError("output must be a LocalTarget")

        # write job output to temporary sparkey file
        temp_output = luigi.LocalTarget(is_tmp=True)
        w = sparkey.LogWriter(temp_output.path)
        for line in infile.open('r'):
            k, v = line.strip().split(self.separator, 1)
            w[k] = v
        w.close()

        # move finished sparkey file to final destination
        temp_output.move(outfile.path)
