import boto3
import json
import requests
from datetime import datetime
import uuid
import sudocoins_logger


log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    inputUrl = event['url']
    userId = event['userId']
    userType = event['userType']

    contractId, tokenId = parseUrl(inputUrl)

    open_sea_url_pattern = "https://api.opensea.io/api/v1/asset/{0}/{1}"
    url = open_sea_url_pattern.format(contractId, tokenId)
    x = requests.get(url)
    open_sea_response = json.loads(x.text)
    log.info(f'quality_score_response: {open_sea_response}')

    msg = {
        'id': str(uuid.uuid1()),
        'redirect': inputUrl,
        'name': open_sea_response['name'],
        'description': open_sea_response['description'],
        "image_url": open_sea_response['image_url'],
        "image_preview_url": open_sea_response['image_preview_url'],
        "image_thumbnail_url": open_sea_response['image_thumbnail_url'],
        "image_original_url": open_sea_response['image_original_url'],
        "animation_url": open_sea_response['animation_url'],
        "animation_original_url": open_sea_response['animation_original_url'],
        "addedBy": userId,
        "userType": userType,
        "timestamp": str(datetime.utcnow().isoformat())
    }

    response = dynamodb.Table('art').put_item(
        Item=msg,
        ReturnValues="ALL_NEW"
    )

    return response


def parseUrl(url):
    # 'https://rarible.com/token/0x672b4d3e393e99661922ff0fb0d6b32be13faba3:1'
    # 'https://opensea.io/assets/0x672b4d3e393e99661922ff0fb0d6b32be13faba3/1'
    if url.find('rarible.com') != -1:
        sub1 = url.find('token/')
        start = sub1 + 6
        rest = url[start:]
        variables = rest.split(':')
        contractId = variables[0]
        tokenId = variables[1]
    elif url.find('opensea.com') != -1:
        sub1 = url.find('assets/')
        start = sub1 + 8
        rest = url[start:]
        variables = rest.split('/')
        contractId = variables[0]
        tokenId = variables[1]
    else:
        contractId = ""
        tokenId = ""

    return contractId, tokenId
