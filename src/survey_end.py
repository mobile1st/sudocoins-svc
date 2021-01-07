import json
import os
import boto3
import hashlib


def lambda_handler(event, context):
    print(event)
    try:
        redirectUrl, hostUrl = getUrls()
        if 'referer' in event["headers"]:
            referer = event["headers"]["referer"]
        else:
            referer = "None"
        msgValue = {
            "referer": referer,
            "queryStringParameters": event["queryStringParameters"],
            "hashState": verifyHash(hostUrl, event["queryStringParameters"]),
            "buyerName": "cint"
        }

        try:
            messageResponse = pushMsg(msgValue)
            print(messageResponse)

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
    # tid=%transaction_id%&h=%signature_hmac_sha%
    shaUrl = params["hash"]
    hashUrl = (hostUrl + "status={0}&sid={1}&tid={2}").format(params["status"], params["sid"], params["tid"])
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
    #  expectedParams = response['Item']["configValue"]["expectedParams"]

    return redirectUrl, hostUrl #  , expectedParams


def checkSha(url, hash):
    hashObject = hashlib.sha256(url.encode('utf-8'))
    hexDig = hashObject.hexdigest()
    if hexDig == hash:
        return True
    else:
        return False


