import boto3
import json
import requests
from datetime import datetime
import uuid
import sudocoins_logger
from art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    try:
        inputUrl = event['url']
        userId = event['userId']

        contractId, tokenId = parseUrl(inputUrl)
        open_sea_response = callOpenSea(contractId, tokenId)

        print("user id")
        print(userId)

        art_uploads_record = art.share(contractId, tokenId, open_sea_response, inputUrl, userId)

        return art_uploads_record

    except Exception as e:
        log.exception(e)


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

