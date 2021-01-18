import json
import os
import boto3
import hashlib
import hmac


def lambda_handler(event, context):
    print(event)
    try:
        redirectUrl, hostUrl = getUrls()
        if 'referer' in event["headers"]:
            referer = event["headers"]["referer"]
        else:
            referer = "None"

        hashState = verifyHash(hostUrl, event["queryStringParameters"])

        msgValue = {
            "referer": referer,
            "queryStringParameters": event["queryStringParameters"],
            "hashState": hashState,
            "buyerName": "cint"
        }

        try:
            messageResponse = pushMsg(msgValue)

            if not msgValue['hashState']:
                msgValue["message"] = "invalid"
            else:
                msgValue["message"] = "valid"

            token = msgValue["message"]

        except Exception as e:
            print(e)
            msgValue['status'] = 'Invalid'
            msgValue["message"] = "invalid"
            token = msgValue["message"]

        response = createRedirect(redirectUrl, token)

        return response

    except Exception as e:
        print(e)
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }

        return response


def createRedirect(redirectUrl, token):
    data = {}
    response = {
        "statusCode": 302,
        "headers": {'Location': redirectUrl + "msg=" + token},
        "body": json.dumps(data)
    }
    return response


def pushMsg(msgValue):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
    record = queue.send_message(MessageBody=json.dumps(msgValue), MessageGroupId='EndTransaction')

    return record


def verifyHash(hostUrl, params):
    shaUrl = params["h"]
    url = (hostUrl + "status={0}&sid={1}&tid={2}").format(params["status"], params["sid"], params["tid"])
    hashState = checkSha(url, shaUrl)
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
    #  expectedParams = response['Item']["configValue"]["expectedParams"]

    return redirectUrl, hostUrl


def checkSha(url, hash):
    secret = os.environ["keyId"]
    signature = hmac.new(
        secret.encode('utf-8'),
        url.encode('utf-8'),
        digestmod=hashlib.sha256
    ).hexdigest()

    if signature == hash:
        return True
    else:
        return False


