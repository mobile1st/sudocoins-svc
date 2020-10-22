import json
import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


def load_profile(user_id):
    profile_table_name = os.environ["PROFILE_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    profile_table = dynamodb.Table(profile_table_name)
    try:
        response = profile_table.query(
            KeyConditionExpression=Key("UserId").eq(user_id),
        )
    except ClientError as e:
        print("Failed to query profile for userId=%s error=%s", user_id, e.response['Error']['Message'])
        return {
            'statusCode': 400,
            'profile': 'Invalid user_id'
        }
    else:
        if "balance" in response['Items'][0].keys():
            balance = response['Items'][0]["balance"]
        else:
            balance = "0"
        if "currency" in response['Items'][0].keys():
            currency = response['Items'][0]["currency"]
        else:
            currency = "USD"
        profile_object = {
            "active": response['Items'][0]["Status"],
            "email": response['Items'][0]["Email"],
            "signupDate": response['Items'][0]["CreatedAt"],
            "UserID": response['Items'][0]["UserId"],
            "balance": balance,
            "currency": currency
        }
        return {
            'statusCode': 200,
            'profile': profile_object
        }


def load_history(user_id):
    ledger_table_name = os.environ["LEDGER_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    ledger_table = dynamodb.Table(ledger_table_name)

    transaction_table_name = os.environ["TRANSACTION_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    transaction_table = dynamodb.Table(transaction_table_name)

    try:
        ledger_history = ledger_table.query(
            KeyConditionExpression=Key("UserId").eq(user_id),
            ScanIndexForward=False)
        '''
        transaction_history = transaction_table.query(
            KeyConditionExpression=Key("UserId").eq(user_id),
            ScanIndexForward=False)

        history = mergeHistory(ledger_history, transaction_history)
        '''

        history = ledger_history
    except ClientError as e:
        print("Failed to query ledger for userId=%s error=%s", user_id, e.response['Error']['Message'])
        return 'error', {}
    else:
        return 'success', history  # json.dumps(ledger_history['Items'])


def mergeHistory(ledger_history, transaction_history):
    history_list = []
    for i in ledger_history['Items']:
        history_list.append(i)
    for k in transaction_history['Items']:
        history_list.append(k)
    return history_list


def get_survey_object(userId):
    config_table_name = os.environ["CONFIG_TABLE"]
    config_key = "TakeSurveyPage"
    dynamodb = boto3.resource('dynamodb')
    config_table = dynamodb.Table(config_table_name)
    URL = "https://cesyiqf0x6.execute-api.us-west-2.amazonaws.com/prod/SudoCoinsTakeSurvey?ip=108.50.251.254"
    try:
        response = config_table.get_item(Key={'configKey': config_key})
    except ClientError as e:
        print("Failed to query config error=%s", e.response['Error']['Message'])
        return None
    else:
        config_data = response['Item']
        buyer_object = []
        for i in config_data['configValue']['publicBuyers']:
            buyer_object.append(config_data['configValue']["buyer"][i])

        survey_tiles = []
        for i in buyer_object:
            object = {
                "name": i["name"],
                "iconLocation": i["iconLocation"],
                "incentive": i["defaultCPI"],
                "URL": URL + "&buyer_name=" + i["name"] + "&user_id=" + userId
            }
            survey_tiles.append(object)
        return survey_tiles

    return None


def lambda_handler(event, context):
    print("event=%s userId=%", event, context.identity.cognito_identity_id)
    json_input = event
    profile_resp = load_profile(json_input["user_id"])
    if profile_resp["statusCode"] != 200:
        return {
            'statusCode': 400,
            'body': {
                "code": 1,
                "error": 'Invalid account'
            }
        }
    history_status, history = load_history(json_input["user_id"])
    if history_status != 'success':
        return {
            'statusCode': 400,
            'body': {
                "code": 2,
                "profile": profile_resp["profile"],
                "history": history
            }
        }

    survey_tile = get_survey_object(json_input["user_id"])
    if survey_tile is None:
        data = {
            "code": 3,
            "profile": profile_resp["profile"],
            "history": history,
            "survey_tile": "Error fetching survey tile"
        }
        return {
            'statusCode': 400,
            'body': data
        }
    return {
        'statusCode': 200,
        'body': {
            "profile": profile_resp["profile"],
            "history": history,
            "survey": survey_tile
        }
    }
