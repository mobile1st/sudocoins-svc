import boto3
import json
import requests
from datetime import datetime
import uuid
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    try:
        inputUrl = event['url']
        userId = event['userId']

        contractId, tokenId = parseUrl(inputUrl)
        open_sea_response = callOpenSea(contractId, tokenId)

        msg = {
            'id': str(uuid.uuid1()),
            "contractId": contractId,
            "tokenId": tokenId,
            'redirect': inputUrl,
            'name': open_sea_response['name'],
            'description': open_sea_response['description'],
            "image_url": open_sea_response['image_url'],
            "image_preview_url": open_sea_response['image_preview_url'],
            "image_thumbnail_url": open_sea_response['image_thumbnail_url'],
            "image_original_url": open_sea_response['image_original_url'],
            "animation_url": open_sea_response['animation_url'],
            "animation_original_url": open_sea_response['animation_original_url'],
            "userId": userId,
            "userType": "user",
            "timestamp": str(datetime.utcnow().isoformat())
        }

        # logic to make sure the user didn't already add this art.

        dynamodb.Table('art').put_item(
            Item=msg
        )
        print("here")
        dynamodb.Table('Profile').update_item(
            Key={'userId': userId},
            UpdateExpression="SET sudocoins = if_not_exists(sudocoins, :start) + :inc",
            ExpressionAttributeValues={
                ':inc': 10,
                ':start': 0
            },
            ReturnValues="UPDATED_NEW"
        )

        return msg

    except Exception as e:
        print(e)
        log.exception('Could not get art')
        return {
            "error": e
        }


def parseUrl(url):
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


def callOpenSea(contractId, tokenId):
    open_sea_url_pattern = "https://api.opensea.io/api/v1/asset/{0}/{1}"
    url = open_sea_url_pattern.format(contractId, tokenId)
    x = requests.get(url)
    open_sea_response = json.loads(x.text)
    log.info(f'open_sea_response: {open_sea_response}')

    return open_sea_response

