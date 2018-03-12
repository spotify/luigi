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
import datetime
import json

import luigi
from luigi.contrib.esindex import CopyToIndex


class FakeDocuments(luigi.Task):
    """
    Generates a local file containing 5 elements of data in JSON format.
    """

    #: the date parameter.
    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        """
        Writes data in JSON format into the task's output target.

        The data objects have the following attributes:

        * `_id` is the default Elasticsearch id field,
        * `text`: the text,
        * `date`: the day when the data was created.

        """
        today = datetime.date.today()
        with self.output().open('w') as output:
            for i in range(5):
                output.write(json.dumps({'_id': i, 'text': 'Hi %s' % i,
                                         'date': str(today)}))
                output.write('\n')

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.

        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.date)


class IndexDocuments(CopyToIndex):
    """
    This task loads JSON data contained in a :py:class:`luigi.target.Target` into an ElasticSearch index.

    This task's input will the target returned by :py:meth:`~.FakeDocuments.output`.

    This class uses :py:meth:`luigi.contrib.esindex.CopyToIndex.run`.

    After running this task you can run:

    .. code-block:: console

        $ curl "localhost:9200/example_index/_search?pretty"

    to see the indexed documents.

    To see the update log, run

    .. code-block:: console

        $ curl "localhost:9200/update_log/_search?q=target_index:example_index&pretty"

    To cleanup both indexes run:

    .. code-block:: console

        $ curl -XDELETE "localhost:9200/example_index"
        $ curl -XDELETE "localhost:9200/update_log/_query?q=target_index:example_index"

    """
    #: date task parameter (default = today)
    date = luigi.DateParameter(default=datetime.date.today())

    #: the name of the index in ElasticSearch to be updated.
    index = 'example_index'
    #: the name of the document type.
    doc_type = 'greetings'
    #: the host running the ElasticSearch service.
    host = 'localhost'
    #: the port used by the ElasticSearch service.
    port = 9200

    def requires(self):
        """
        This task's dependencies:

        * :py:class:`~.FakeDocuments`

        :return: object (:py:class:`luigi.task.Task`)
        """
        return FakeDocuments()


if __name__ == "__main__":
    luigi.run(['IndexDocuments', '--local-scheduler'])
