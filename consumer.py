import json
import boto3

import logging
from botocore.exceptions import ClientError


class KinesisStream:
    """Encapsulates a Kinesis stream."""
    def __init__(self, kinesis_client, stream_name, data_stream_details):
        """
        :param kinesis_client: A Boto3 Kinesis client.
        """
        self.kinesis_client = kinesis_client
        self.stream_name = stream_name
        self.data_stream_details = data_stream_details
        self.stream_exists_waiter = kinesis_client.get_waiter('stream_exists')

    def get_records(self, max_records):
        """
        Gets records from the stream. This function is a generator that first gets
        a shard iterator for the stream, then uses the shard iterator to get records
        in batches from the stream. Each batch of records is yielded back to the
        caller until the specified maximum number of records has been retrieved.

        :param max_records: The maximum number of records to retrieve.
        :return: Yields the current batch of retrieved records.
        """
        record_count = 0
        try:
            response = self.kinesis_client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=self.data_stream_details['Shards'][0]['ShardId'],
                ShardIteratorType='TRIM_HORIZON'
            )
            shard_iter = response['ShardIterator']
            while record_count < max_records:
                response = self.kinesis_client.get_records(
                    ShardIterator=shard_iter,
                    Limit=1
                )
                shard_iter = response['NextShardIterator']
                records = response['Records']
                if records:
                    print(json.loads(records[0]['Data']))
                    record_count += 1
        except ClientError:
            logger.exception("Couldn't get records from stream %s.", self.stream_name)
            raise
        except KeyboardInterrupt:
            logger.info("Keyboard Interrupt : Closing consumer...")
        finally:
            print(f"Total Records Read : {record_count}")


if __name__ == '__main__':
    logger = logging.getLogger('tipper')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    stream_name = "test-stream"
    kinesis_client = boto3.client('kinesis')
    get_max_records = 100
    data_stream_details = {
        "Shards": [
            {"ShardId": "shardId-00000000000"}
        ]
    }

    ks = KinesisStream(kinesis_client, stream_name, data_stream_details)

    ks.get_records(get_max_records)
