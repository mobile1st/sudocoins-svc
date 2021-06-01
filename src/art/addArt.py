import boto3
import json
import http.client
import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        inputUrl = body['url']
        userId = body['userId']

        contractId, tokenId = parseUrl(inputUrl)
        open_sea_response = callOpenSea(contractId, tokenId)

        art_uploads_record = art.add(contractId, tokenId, open_sea_response, inputUrl, userId)

        return art_uploads_record


    except Exception as e:
        log.exception(e)
        return {
            "error": True,
            "msg": "Sorry, your input doesn't return any Art. Please Add another Art."
        }


def parseUrl(url):
    if url.find('rarible.com') != -1:
        sub1 = url.find('token/')
        start = sub1 + 6
        rest = url[start:]
        variables = rest.split(':')
        contractId = variables[0]
        tokenId = variables[1]
    elif url.find('opensea.io') != -1:
        sub1 = url.find('assets/')
        start = sub1 + 7
        rest = url[start:]
        variables = rest.split('/')
        contractId = variables[0]
        tokenId = variables[1]
    else:
        contractId = ""
        tokenId = ""

    return contractId, tokenId


def callOpenSea(contractId, tokenId):
    open_sea_url_pattern = "/api/v1/asset/{0}/{1}"
    path = open_sea_url_pattern.format(contractId, tokenId)
    conn = http.client.HTTPSConnection("api.opensea.io")
    conn.request("GET", path)
    response = conn.getresponse()
    open_sea_response = json.loads(response.read())
    log.info(f'open_sea_response: {open_sea_response}')

    return open_sea_response

