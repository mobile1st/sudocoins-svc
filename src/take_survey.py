import os
import boto3
from botocore.exceptions import ClientError
import json
from buyerRedirect import BuyerRedirect
import history


def lambda_handler(event, context):
    params = event["queryStringParameters"]

    try:
        ip = event['requestContext']['identity']['sourceIp']
    except Exception as e:
        ip = ""

    print(ip)

    try:
        if 'userId' in params:
            userId = params['userId']
        elif 'sub' in params:
            dynamodb = boto3.resource('dynamodb')
            subTable = dynamodb.Table('sub')
            subResponse = subTable.get_item(Key={'sub': params['sub']})
            userId = subResponse['Item']['userId']
        else:
            response = {
                "statusCode": 302,
                "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
                "body": json.dumps({})
            }
            return response

    except Exception as e:
        print(e)
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }

        return response

    try:
        dynamodb = boto3.resource('dynamodb')
        transaction = history.History(dynamodb)
        data = transaction.insertTransactionRecord(userId, params['buyerName'], ip)
        print("transaction record inserted")

    except Exception as e:
        print(e)
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }

        return response

    try:
        entryUrl = generateEntryUrl(userId, params['buyerName'], data["transactionId"], ip)
        print("entryUrl generated")
        body = {}
        response = {"statusCode": 302, "headers": {'Location': entryUrl}, "body": json.dumps(body)}

        return response

    except Exception as e:
        print(e)
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }
        return response


def getSurveyObject(buyerName):
    """Fetches information about a particular survey to generate entry url
    Argument: buyer name
    Returns: config for buyer to build survey entry url
    """
    dynamodb = boto3.resource('dynamodb')
    configTableName = os.environ["CONFIG_TABLE"]
    configTable = dynamodb.Table(configTableName)
    configKey = "TakeSurveyPage"

    try:
        response = configTable.get_item(Key={'configKey': configKey})

    except ClientError as e:
        print(e.response['Error']['Message'])
        return None

    else:
        try:
            configData = response['Item']["configValue"]
            if buyerName in configData["buyer"].keys():
                buyerObject = configData["buyer"][buyerName]

                return buyerObject

        except Exception as e:
            print(e)
            return None

    return None


def generateEntryUrl(userId, buyerName, transactionId, ip):
    dynamodb = boto3.resource('dynamodb')

    survey = getSurveyObject(buyerName)
    if survey is None:
        return None

    else:
        redirect = BuyerRedirect(dynamodb)
        entryUrl = redirect.getRedirect(userId, buyerName, survey, ip, transactionId)

    return entryUrl




