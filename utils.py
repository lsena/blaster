import os
import re
from functools import lru_cache

import aioboto3
from azure.storage.blob import BlobServiceClient

from config import Settings


@lru_cache()
def get_settings():
    return Settings()


def download_from_blob_storage(container, filename):
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container, blob=filename)

    download_file_path = f'data/vespa/{filename}'
    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())


def download_remote_data(url):
    result = re.search(r"(.*):\/\/(.*)\/(.*)", url)
    provider = result.group(0)
    container = result.group(1)
    filename = result.group(2)
    if provider == 'blob':
        download_from_blob_storage(container, filename)


async def poll_sqs_queue():
    # Create SQS client
    sqs = aioboto3.client('sqs')

    queue_url = get_settings().sqs_queue_url

    while True:
        # Receive message from SQS queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=400_000,
            WaitTimeSeconds=10
        )

        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        yield message
        # await process_sqs_msg(message)
        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
