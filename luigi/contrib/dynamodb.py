import datetime

from botocore.exceptions import ClientError

import luigi


class DynamoDBTarget(luigi.Target):
    """
    Target for a resource in DynamoDB.

    """
    marker_table = luigi.configuration.get_config().get(
        'dynamodb', 'marker-table', 'table_updates')
    read_capacity_units = luigi.configuration.get_config().get(
        'dynamodb', 'read-capacity-units', 1)
    write_capacity_units = luigi.configuration.get_config().get(
        'dynamodb', 'write-capacity-units', 1)

    def __init__(self, resource, update_id):
        """
        Args:
            resource (DynamoDB.ServiceResource): created using boto3.resource()
            update_id (str): An identifier for this dataset 
        """
        self.update_id = update_id
        self.resource = resource

    def touch(self):
        """ Mark this update as complete """
        self.create_marker_table_if_not_exists()

        table = self.resource.Table(self.marker_table)
        item = {'update_id': self.update_id,
                'inserted': str(datetime.datetime.now())}
        table.put_item(Item=item)

    def exists(self):
        table = self.resource.Table(self.marker_table)

        try:
            response = table.get_item(Key={'update_id': self.update_id})
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            else:
                raise
        return response.get('Item') is not None

    def create_marker_table_if_not_exists(self):
        try:
            self.resource.meta.client.describe_table(
                TableName=self.marker_table)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                self.resource.create_table(
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'update_id',
                            'AttributeType': 'S'
                        }
                    ],
                    TableName=self.marker_table,
                    KeySchema=[
                        {
                            'AttributeName': 'update_id',
                            'KeyType': 'HASH'
                        }
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': int(self.read_capacity_units),
                        'WriteCapacityUnits': int(self.write_capacity_units)
                    })
                # AWS might take a while before the table is created
                # A waiter polls describe_table every 20 seconds 10 times
                # before it raises an error
                waiter = self.resource.meta.client.get_waiter('table_exists')
                waiter.wait(TableName=self.marker_table)
