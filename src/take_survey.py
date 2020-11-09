import uuid
from datetime import datetime
import os
import boto3
from botocore.exceptions import ClientError
import json


def getSurveyObject(buyerName):
    """Fetches information about a particular survey to generate entry url
    Arguments: buyer name
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


def takeSurvey(params, ip):
    """Generates a transaction and ledger record for the user
    Arguments: event parameters including userId, buyerName, and ip
    Returns: validation that records were successfully created"""
    userId = params["userId"]
    buyerName = params["buyerName"]
    transactionId = uuid.uuid1()
    started = datetime.utcnow().isoformat()
    surveyData = []
    # create start transaction
    try:
        transactionData = {
            'transactionId': str(transactionId),
            "userId": userId,
            'status': "started",
            'ip': ip,
            'started': str(started),
            'buyer': buyerName
        }
        dynamodb = boto3.resource('dynamodb')
        transactionTable = dynamodb.Table(os.environ["TRANSACTION_TABLE"])
        transactionResponse = transactionTable.put_item(
            Item=transactionData
        )
        surveyData.append(transactionData)

    except Exception as e:
        print(f'Create Transaction start record Failed: {e}')

    try:
        ledgerData = {
            "userId": userId,
            'transactionId': str(transactionId),
            'type': "Survey",
            'status': "Started",
            'lastUpdate': str(started),
            'ip': ip,
            'createdAt': str(started)
        }
        dynamodb = boto3.resource('dynamodb')
        ledgerTable = dynamodb.Table(os.environ["LEDGER_TABLE"])
        ledgerResponse = ledgerTable.put_item(
            Item=ledgerData
        )
        surveyData.append(ledgerData)

    except Exception as e:
        print(f'Create Ledger record Failed: {e}')

    return surveyData


def generateEntryUrl(params, transactionId, ip):
    """Generates entry url that redirects user to the buyer's platform
    Arguments: event params including userId and buyerName, transactionId generated, ip
    Returns: redirect url"""
    userId = params["userId"]
    buyerName = params["buyerName"]
    survey = getSurveyObject(buyerName)
    if survey is None:
        return None

    entryUrl = "{0}?si={1}&ssi={2}&unique_user_id={3}&ip={4}".format(survey['url'], survey['appId'], transactionId,
                                                                     userId, ip)

    return entryUrl


def lambda_handler(event, context):
    params = event["queryStringParameters"]
    ip = event['requestContext']['identity']['sourceIp']
    try:
        data = takeSurvey(params, ip)
        if len(data) == 0:
            return {
                'statusCode': 400,
                'body': json.dumps({"data": data})
            }
    except Exception as e:
        print(e)
        pass

    try:
        entryUrl = generateEntryUrl(params, data[0]["transactionId"], ip)
        if entryUrl is None:
            return {
                'statusCode': 400,
                'body': json.dumps({"entry urL": "error generating entry url"})
            }

        body = {}
        response = {"statusCode": 302, "headers": {'Location': entryUrl}, "body": json.dumps(body)}

        return response

    except Exception as e:
        print(e)
        """redirect user back to profile page with error message"""
        return e


