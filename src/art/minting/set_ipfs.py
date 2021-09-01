import boto3
from util import sudocoins_logger
import json
import requests

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    # body = json.loads(event['body'])
    body = event
    file_name = body['file_name']

    s3 = boto3.resource('s3')
    tmp = "/tmp/" + file_name
    s3.meta.client.download_file('sudocoins-art-bucket', file_name, tmp)

    with open(tmp, 'rb') as file:
        files = {
            'file': file
        }
        response = requests.post('https://ipfs.infura.io:5001/api/v0/add', files=files,
                                 auth=('1xBSq6KuqrbDmhs2ASr722Cs8JF', '3c98ebb9f76fabc45d519718e41dd4f0'))

        response2 = json.loads(response.text)

        return {
            "ipfs_image": response2['Hash']
        }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


