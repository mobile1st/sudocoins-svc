import os
import boto3
from botocore.exceptions import ClientError
import json
from buyerRedirect import BuyerRedirect
import history
from datetime import datetime
import sudocoins_logger

log = sudocoins_logger.get()
sns_client = boto3.client('sns')


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    if event.get('queryStringParameters') is None:
        log.warn('the request does not contain query parameters')
        return {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }
    params = event['queryStringParameters']
    try:
        ip = event['requestContext']['identity']['sourceIp']
    except Exception as e:
        ip = ""

    log.info(f'IP: {ip}')

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

    except Exception:
        log.exception('could not retrieve user data')
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }

        return response

    try:
        dynamodb = boto3.resource('dynamodb')
        transaction = history.History(dynamodb)
        data, profile = transaction.insertTransactionRecord(userId, params['buyerName'], ip)
        log.debug('transaction record inserted')
        timestamp = datetime.utcnow().isoformat()
        sns_client.publish(
            TopicArn="arn:aws:sns:us-west-2:977566059069:transaction-event",
            MessageStructure='string',
            MessageAttributes={
                'source': {
                    'DataType': 'String',
                    'StringValue': 'SURVEY_START'
                }
            },
            Message=json.dumps({
                'userId': userId,
                'source': 'SURVEY_START',
                'status': 'Started',
                'awsRequestId': context['aws_request_id'],
                'timestamp': timestamp,
                'transactionId': data.get('transactionId'),
                'buyerName': params.get('buyerName'),
            })
        )
        log.debug('start added to sns')

    except Exception as e:
        log.exception(e)
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }

        return response

    try:
        log.info(f'buyer: {params["buyerName"]}')
        entryUrl = generateEntryUrl(userId, params['buyerName'], data["transactionId"], ip, profile)
        log.debug('entryUrl generated')
        body = {}
        response = {"statusCode": 302, "headers": {'Location': entryUrl}, "body": json.dumps(body)}

        return response

    except Exception as e:
        log.exception(e)
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }
        return response


def getSurveyObject(buyerName):
    dynamodb = boto3.resource('dynamodb')
    configTableName = os.environ["CONFIG_TABLE"]
    configTable = dynamodb.Table(configTableName)
    configKey = "TakeSurveyPage"

    try:
        response = configTable.get_item(Key={'configKey': configKey})

    except ClientError as e:
        log.warn(e.response['Error']['Message'])
        return None

    else:
        try:
            configData = response['Item']["configValue"]
            if buyerName in configData["buyer"].keys():
                buyerObject = configData["buyer"][buyerName]

                return buyerObject

        except Exception as e:
            log.exception(e)
            return None

    return None


def generateEntryUrl(userId, buyerName, transactionId, ip, profile):
    try:
        dynamodb = boto3.resource('dynamodb')
        survey = getSurveyObject(buyerName)
        if survey is None:
            return None

        else:
            redirect = BuyerRedirect(dynamodb)
            entryUrl = redirect.getRedirect(userId, buyerName, survey, ip, transactionId, profile)

        return entryUrl
    except Exception as e:
        log.exception(e)


'''
def parseAccLang(headers):
    acceptLanguage = headers['accept-language'][0]
    part1 = acceptLanguage.split(',')[0]
    country = part1.split('-')
    cc = {}
    if len(country) == 2:
        cc['lang'] = country[0]
        cc['cc'] = country[1]
    else:
        cc['lang'] = country[0]

    return cc
'''
