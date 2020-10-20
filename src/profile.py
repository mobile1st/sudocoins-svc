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
        return {
            'statusCode': 200,
            'profile': response['Items']
        }


def load_history(user_id):
    ledger_table_name = os.environ["LEDGER_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    ledger_table = dynamodb.Table(ledger_table_name)
    try:
        response = ledger_table.query(
            IndexName="UserId-CreatedAt-index",
            KeyConditionExpression=Key("UserId").eq(user_id),
            ScanIndexForward=False)

    except ClientError as e:
        print("Failed to query ledger for userId=%s error=%s", user_id, e.response['Error']['Message'])
        return 'error', {}
    else:
        return 'success', json.dumps(response['Items'])


def get_survey_object(buyer_name):
    config_table_name = os.environ["CONFIG_TABLE"]
    config_key = "TakeSurveyPage"
    dynamodb = boto3.resource('dynamodb')
    config_table = dynamodb.Table(config_table_name)
    try:
        response = config_table.get_item(Key={'configKey': config_key})
    except ClientError as e:
        print("Failed to query config for buyer=%s error=%s", buyer_name, e.response['Error']['Message'])
        return None
    else:
        config_data = response['Item']
        if buyer_name in config_data['configValue']["buyer"].keys():
            buyer_object = config_data['configValue']["buyer"][buyer_name]
            return buyer_object
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

    survey_tile = get_survey_object(json_input["name"])
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
