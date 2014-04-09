import luigi
import json
import luigi.notifications

from unittest import TestCase
from luigi.contrib import redshift 
from moto import mock_s3
from boto.s3.key import Key
from luigi.s3 import S3Client

luigi.notifications.DEBUG = True

AWS_ACCESS_KEY = 'key'
AWS_SECRET_KEY = 'secret'

BUCKET = 'bucket'
KEY = 'key'
FILES = ['file1', 'file2', 'file3']


def generate_manifest_json(path_to_folder, file_names):
    entries = []
    for file_name in file_names:
        entries.append({
            'url' : '%s/%s' % (path_to_folder, file_name),
            'mandatory': True
            })
    return {'entries' : entries} 

class TestRedshiftManifestTask(TestCase):

    @mock_s3 
    def test_run(self):
        client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
        bucket = client.s3.create_bucket(BUCKET)
        for key in FILES:
            k = Key(bucket)
            k.key = '%s/%s' % (KEY, key)
            k.set_contents_from_string('')
        folder_path = 's3://%s/%s' % (BUCKET, KEY)
        k = Key(bucket)
        k.key = 'manifest'
        path = 's3://%s/%s/%s' % (BUCKET, k.key, 'test.manifest')
        t = redshift.RedshiftManifestTask(path, folder_path)
        luigi.build([t], local_scheduler=True)

        output = t.output().open('r').read()
        expected_manifest_output = json.dumps(generate_manifest_json(folder_path,FILES))
        self.assertEqual(output,expected_manifest_output )

