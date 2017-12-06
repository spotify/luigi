"""
Example Databricks job pipeline
Illustrates cluster creation and deletion, and submission of a job task

Requirements:
- local luigi.cfg file with [databricks] populated
- Access to a Databricks account (Community edition works if only testing cluster
creation/deletion)
- Sample notebook (update `notebook_path` parameter in job definition)

This pipeline will create and delete an interactive cluster, then run a
specified notebook as a job within a new cluster.

"""

from json import dump, dumps

import luigi
import luigi.contrib.databricks as lcd


class CreateTestCluster(lcd.CreateDatabricksClusterTask):
    """ test """

    cluster_name = 'testlust'
    node_type_id = 'm4.large'
    spark_version = '3.3.x-scala2.11'
    num_workers = 2
    cluster_params = {
        'aws_attributes': {
            'ebs_volume_size': 100,
            'ebs_volume_count': 1,
            'ebs_volume_type': 'GENERAL_PURPOSE_SSD'
        },
        'spark_conf': {}
    }

    def signal_complete(self, out_data):
        """ write out cluster config """

        with self.output().open('w') as fopen:
            dump(out_data, fopen)

    def output(self):
        """ output target """

        return luigi.LocalTarget('/tmp/dbclustercreateexample1.json')


class DeleteTestCluster(lcd.DeleteDatabricksClusterTask):
    """ test """

    def requires(self):
        return {
            'ClusterCreate': CreateTestCluster()
        }

    def signal_complete(self):
        """ write out cluster config """

        with self.output().open('w') as fopen:
            dump({'cluster_id': self.cluster_id}, fopen)

    def output(self):
        """ output target """

        return luigi.LocalTarget('/tmp/dbclusterdeleteexample1.json')


class SubmitTestNotebookJob(lcd.DatabricksNotebookTask):
    """ test """

    notebook_task = {
        'notebook_path': '/Users/<MY USER ACCOUNT>/test_notebook_submit',
        'base_parameters': {}
    }

    node_type_id = 'm4.large'
    spark_version = '3.3.x-scala2.11'
    num_workers = 2
    cluster_params = {
        'aws_attributes': {
            'ebs_volume_size': 100,
            'ebs_volume_count': 1,
            'ebs_volume_type': 'GENERAL_PURPOSE_SSD'
        },
        'spark_conf': {}
    }

    def signal_complete(self):
        """ write out cluster config """

        with self.output().open('w') as fopen:
            dump({'notebook_path': self.notebook_path}, fopen)

    def output(self):
        """ output target """

        return luigi.LocalTarget('/tmp/dbnotebookjobex1.json')


class DatabricksExample(luigi.WrapperTask):
    """ test pipeline; demos creating/deleting a cluster, and submitting a job """

    def requires(self):

        yield DeleteTestCluster()
        yield SubmitTestNotebookJob()


if __name__ == '__main__':
    luigi.run()
