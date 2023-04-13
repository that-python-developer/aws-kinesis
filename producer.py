import json
import boto3

import logging
from botocore.exceptions import ClientError


class KinesisStream:
    """Encapsulates a Kinesis stream."""
    def __init__(self, kinesis_client, stream_name):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = kinesis_client
        self.stream_name = stream_name
        self.details = None
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    def put_record(self, data, partition_key):
        """
        Puts data into the stream. The data is formatted as JSON before it is passed
        to the stream.

        :param data: The data to put in the stream.
        :param partition_key: The partition key to use for the data.
        :return: Metadata about the record, including its shard ID and sequence number.
        """
        try:
            response = self.kinesis_client.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(data),
                PartitionKey=partition_key
            )
            print(f"PRODUCED -> Stream : {self.stream_name} -- Record : {json.dumps(data)}")
        except ClientError:
            logger.exception("Couldn't put record in stream %s.", self.stream_name)
            raise
        else:
            return response


if __name__ == '__main__':
    logger = logging.getLogger('tipper')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    stream_name = "test-stream"
    kinesis_client = boto3.client('kinesis')

    ks = KinesisStream(kinesis_client, stream_name)

    data = {
        "customer_id": 3,
        "customer_name": "John",
        "product_id": 3,
        "product_qty": 7
    }
    ks.put_record(data, "1")
