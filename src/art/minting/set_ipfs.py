import boto3
from util import sudocoins_logger
import json
import requests

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.info(f'event {event}')
    body = json.loads(event['body'])
    #  body = event
    file_name = body['file_name']
    name = body['name']
    description = body['description']

    art_id = file_name.split(".")[0]

    ipfs_image = set_ipfs_image(file_name)
    ipfs_meta = generate_meta_data(name, description, ipfs_image["ipfs_image"], art_id)

    return {
        "uri": ipfs_meta
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def set_ipfs_image(file_name):
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


def generate_meta_data(name, description, image, art_id):
    data = {
        "name": name,
        "description": description,
        "image": "ipfs://ipfs/" + image,
        "external_url": "https://app.sudocoins.com/art/social/" + art_id
    }

    with open('/tmp/metadata.json', 'w') as outfile:
        json.dump(data, outfile)

    with open('/tmp/metadata.json', 'rb') as meta_json:
        files = {
            'file': meta_json
        }
        response = requests.post('https://ipfs.infura.io:5001/api/v0/add', files=files,
                                 auth=('1xBSq6KuqrbDmhs2ASr722Cs8JF', '3c98ebb9f76fabc45d519718e41dd4f0'))

    ipfs_meta_hash = json.loads(response.text)

    log.info(f'ipfs response {ipfs_meta_hash}')

    return ipfs_meta_hash['Hash']

