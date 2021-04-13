import boto3
import json
from buyerRedirect import BuyerRedirect
import history
from datetime import datetime
import sudocoins_logger
import requests

log = sudocoins_logger.get()
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

buyer_redirect = BuyerRedirect(dynamodb)
transaction_history = history.History(dynamodb)

quality_score_url_pattern = 'https://ipqualityscore.com/api/json/ip/AnfjI4VR0v2VxiEV5S8c9VdRatbJR4vT/{0}?strictness=1&allow_public_access_points=true'
invalid_response = {"statusCode": 302, "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'}, "body": '{}'}


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    if event.get('queryStringParameters') is None:
        log.warn('the request does not contain query parameters')
        return invalid_response

    try:
        params = event['queryStringParameters']
        ip = event.get('requestContext', {}).get('identity', {}).get('sourceIp')
        user_id = get_user_id(params)
        fraud_score, ipqs = get_quality_score(ip)

        if fraud_score and fraud_score > 100:  # fraud check is optional
            log.warn(f"userId: {user_id} is considered suspicious")
            dynamodb.Table('Profile').update_item(
                Key={"userId": user_id},
                UpdateExpression="set fraud_score=:fs, active=:ac",
                ExpressionAttributeValues={
                    ":fs": str(fraud_score),
                    ":ac": False
                },
                ReturnValues="ALL_NEW"
            )
            return invalid_response

        data, profile = transaction_history.insertTransactionRecord(user_id, params['buyerName'], ip, fraud_score, ipqs)
        log.info(f"created transaction: {json.dumps(data)}")

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
                'userId': user_id,
                'source': 'SURVEY_START',
                'status': 'Started',
                'awsRequestId': context.aws_request_id,
                'timestamp': datetime.utcnow().isoformat(),
                'transactionId': data.get('transactionId'),
                'buyerName': params.get('buyerName'),
            })
        )

        entry_url = generate_entry_url(user_id, params['buyerName'], data["transactionId"], ip, profile)
        return {"statusCode": 302, "headers": {'Location': entry_url}, "body": '{}'}

    except Exception as e:
        log.exception(e)
        return invalid_response


def get_user_id(params):
    if 'userId' in params:
        log.info(f"userId from query.userId: {params['userId']}")
        return params['userId']
    elif 'sub' in params:
        sub_table = dynamodb.Table('sub')
        sub_results = sub_table.get_item(Key={'sub': params['sub']})
        user_id = sub_results['Item']['userId']
        log.info(f"userId from query.sub: {params['sub']} -> userId: {user_id}")
        return user_id

    raise Exception(f'userId was not determined from params: {params}')


def get_quality_score(ip):
    if not ip or ip == '':
        log.info(f'No IP for executing quality score check')
        return None, None

    try:
        log.info(f'IP: {ip} executing quality score check')
        url = quality_score_url_pattern.format(ip)
        x = requests.get(url)
        quality_score_response = json.loads(x.text)
        log.info(f'quality_score_response: {quality_score_response}')
        return quality_score_response["fraud_score"], quality_score_response
    except Exception as e:
        log.exception(e)
        return None, None


def get_survey(buyer):
    response = dynamodb.Table('Config').get_item(Key={'configKey': 'TakeSurveyPage'})
    config = response['Item']["configValue"]
    if buyer not in config["buyer"]:
        raise Exception(f'Could not find configuration for buyer: {buyer}')

    return config["buyer"][buyer]


def generate_entry_url(user_id, buyer_name, transaction_id, ip, profile):
    survey = get_survey(buyer_name)
    entry_url = buyer_redirect.getRedirect(user_id, buyer_name, survey, ip, transaction_id, profile)
    if not entry_url or entry_url == '':
        raise Exception(f'could generate entry url for buyer: {buyer_name} survey: {json.dumps(survey)}')

    log.debug(f'generated entryUrl: {entry_url} for buyer: {buyer_name}')
    return entry_url
