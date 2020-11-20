import json
import os
import boto3



def lambda_handler(event, context):
    try:
        redirectUrl, expectedParams = getUrls()

        if 'referer' in event["headers"]:
            referer = event["headers"]["referer"]
        else:
            referer = ""

        msgValue = {
            "referer": referer,
            "queryStringParameters": event["queryStringParameters"],
            "missingParams": missingParams(event["queryStringParameters"], expectedParams)
        }

        token = "valid"

        response = createRedirect(redirectUrl, token)

        print(msgValue)
        print(event)

        return response



    except Exception as e:
        print(event)
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


def getUrls():
    dynamodb = boto3.resource('dynamodb')
    configTableName = os.environ["CONFIG_TABLE"]
    configTable = dynamodb.Table(configTableName)
    configKey = 'dynataRedirect'

    response = configTable.get_item(Key={'configKey': configKey})
    redirectUrl = response['Item']["configValue"]["redirectUrl"]
    expectedParams = response['Item']["configValue"]["expectedParams"]

    return redirectUrl, expectedParams


def missingParams(params, expectedParams):
    receivedParams = params.keys()
    missingParams = []
    for i in expectedParams:
        if i not in receivedParams:
            missingParams.append(i)
    return missingParams








