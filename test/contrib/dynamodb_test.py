
import boto3
from botocore.exceptions import ClientError

from helpers import unittest
from luigi.contrib.dynamodb import DynamoDBTarget
from moto import mock_dynamodb2


@mock_dynamodb2
class TestDynamoClient(unittest.TestCase):

    client = boto3.resource('dynamodb', region_name='eu-west-1')

    def test_exists_raises_client_error_if_table_doesnt_exist(self):
        target = DynamoDBTarget(self.client, 'some_update_id')
        with self.assertRaises(ClientError):
            target.exists()

    def test_exists_return_true_when_target_exists(self):
        target = DynamoDBTarget(self.client, 'some_update_id')
        target.touch()
        self.assertTrue(target.exists())

    def test_exists_return_false_when_target_doesnt_exit(self):
        target = DynamoDBTarget(self.client, 'some_update_id')
        target.create_marker_table_if_not_exists()
        self.assertFalse(target.exists())
