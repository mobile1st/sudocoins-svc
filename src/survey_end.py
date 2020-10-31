import json
import os
import boto3
import hashlib
import base64


def lambda_handler(event, context):

    try:
        redirectUrl, hostUrl, expectedParams = getUrls()
        msgValue = {
            "referer": event["headers"]["referer"],
            "queryStringParameters": event["queryStringParameters"],
            "hashState": verifyHash(hostUrl, event["queryStringParameters"]),
            "missingParams": missingParams(event["queryStringParameters"], expectedParams)
        }

        try:
            messageResponse = pushMsg(msgValue)
            print(messageResponse)

        except Exception as e:
            print(e)
            msgValue["status"] = "Invalid transactionId"

        try:
            token = encryptMsg(msgValue)

        except Exception as e:
            print(e)
            token = ""

    except Exception as e:
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com'},
            "body": json.dumps({})
        }

    response = createRedirect(redirectUrl, token)
    print(msgValue["hashState"])
    print(msgValue["queryStringParameters"]['c'])
    return response


def createRedirect(redirectUrl, token):
    data = {}
    response = {
        "statusCode": 302,
        "headers": {'Location': redirectUrl + "msg=" + token},
        "body": json.dumps(data)
    }
    return response


def encryptMsg(msgValue):
    kmsClient = boto3.client('kms')
    secret = json.dumps(msgValue, indent=2).encode('utf-8')
    token = encrypt(kmsClient, secret, os.environ["keyId"])
    return token


def pushMsg(msgValue):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
    record = queue.send_message(MessageBody=json.dumps(msgValue), MessageGroupId='EndTransaction')

    return record


def missingParams(params, expectedParams):
    receivedParams = params.keys()
    missingParams = []
    for i in expectedParams:
        if i not in receivedParams:
            missingParams.append(i)
    return missingParams


def verifyHash(hostUrl, params):
    shaUrl = params["h"]
    hashUrl = (hostUrl + "ts={0}&t={1}&c={2}&ip={3}").format(params["ts"], params["t"], params["c"], params["ip"])
    hashState = checkSha(hashUrl, shaUrl)
    if hashState:
        return True
    else:
        return False


def getUrls():
    dynamodb = boto3.resource('dynamodb')
    configTableName = os.environ["CONFIG_TABLE"]
    configTable = dynamodb.Table(configTableName)
    configKey = "surveyEnd"

    response = configTable.get_item(Key={'configKey': configKey})
    redirectUrl = response['Item']["configValue"]["redirectUrl"]
    hostUrl = response['Item']["configValue"]["hostUrl"]
    expectedParams = response['Item']["configValue"]["expectedParams"]

    return redirectUrl, hostUrl, expectedParams


def checkSha(url, hash):
    hashObject = hashlib.sha256(url.encode('utf-8'))
    hexDig = hashObject.hexdigest()
    if hexDig == hash:
        return True
    else:
        return False


def encrypt(client, secret, keyId):
    ciphertext = client.encrypt(
        KeyId=keyId,
        Plaintext=bytes(secret),
    )
    token = base64.b64encode(ciphertext["CiphertextBlob"]).decode("utf-8")

    return token

