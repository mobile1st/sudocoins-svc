import uuid
from datetime import datetime
import os
import boto3
from botocore.exceptions import ClientError
import json


def getSurveyObject(buyerName):
    configTableName = os.environ["CONFIG_TABLE"]
    configKey = "TakeSurveyPage"
    dynamodb = boto3.resource('dynamodb')
    configTable = dynamodb.Table(configTableName)
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


def takeSurvey(params):
    userId = params["userId"]
    buyerName = params["buyerName"]
    transactionId = uuid.uuid1()
    started = datetime.utcnow().isoformat()
    surveyData = []
    #. ip = params["ip"]
    #. survey_object = get_survey_object(buyerName)
    #. revenue = survey_object["defaultCPI"]
    # create start transaction
    try:
        transactionData = {
            'transactionId': str(transactionId),
            "userId": userId,
            'status': "started",
            #. 'ip': ip,
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

    # create ledger transaction
    try:
        ledgerData = {
            "userId": userId,
            'transactionId': str(transactionId),
            'type': "Survey",
            'status': "Started",
            'lastUpdate': str(started) #. ,'ip': ip,
        }
        dynamodb = boto3.resource('dynamodb')
        ledgerTable = dynamodb.Table(os.environ["LEDGER_TABLE"])
        ledgerResponse = ledgerTable.put_item(
            Item=ledgerData
        )
        transactionData.append(ledgerData)

    except Exception as e:
        print(f'Create Ledger record Failed: {e}')

    return surveyData


def generateEntryUrl(params, transactionId):
    userId = params["userId"]
    buyerName = params["buyerName"]
    survey = getSurveyObject(buyerName)
    if survey is None:
        return None
    entryUrl = "{0}?si={1}&ssi={2}&unique_user_id={3}".format(survey['url'], survey['appId'], transactionId, userId)
    return entryUrl


def lambda_handler(event, context):
    params = event
    data = takeSurvey(params)
    print(data)
    if len(data) == 0:
        return {
            'statusCode': 400,
            'body': json.dumps({"data": data})
        }
    entryUrl = generateEntryUrl(params, data[0]["transactionId"])
    if entryUrl is None:
        return {
            'statusCode': 400,
            'body': json.dumps({"entry urL": "error generating entry url"})
        }
    return {
        'statusCode': 200,
        'body': json.dumps({"redirect": entryUrl})
    }
