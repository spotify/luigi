# coding: utf-8

from luigi.contrib.esindex import CopyToIndex
import datetime
import json
import luigi

class FakeDocuments(luigi.Task):
    """ Generate some documents to index. """

    date = luigi.DateParameter(default=datetime.date.today())

    def run(self):
        """ Write line-delimited json. `_id` is the default Elasticsearch id
        field. """
        today = datetime.date.today()
        with self.output().open('w') as output:
            for i in range(5):
                output.write(json.dumps({'_id': i, 'text': 'Hi %s' % i,
                                         'date': str(today)}))
                output.write('\n')

    def output(self):
        return luigi.LocalTarget(path='/tmp/_docs-%s.ldj' % self.date)

class IndexDocuments(CopyToIndex):
    """
    Run

        $ curl "localhost:9200/example_index/_search?pretty"

    after this task, to see the indexed documents. To see the update log, run

        $ curl "localhost:9200/update_log/_search?q=target_index:example_index&pretty"

    To cleanup both indexes run:

        $ curl -XDELETE "localhost:9200/example_index"
        $ curl -XDELETE "localhost:9200/update_log/_query?q=target_index:example_index"
    """
    date = luigi.DateParameter(default=datetime.date.today())

    index = 'example_index'
    doc_type = 'greetings'
    host = 'localhost'
    port = 9200

    def requires(self):
        return FakeDocuments()

if __name__ == "__main__":
    luigi.run(['--task', 'IndexDocuments'], use_optparse=True)
