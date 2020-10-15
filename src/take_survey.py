import uuid
from datetime import datetime
import os
import boto3
from botocore.exceptions import ClientError


def get_survey_object(buyer_name):
    config_table_name = os.environ["CONFIG_TABLE"]
    config_key = "TakeSurveyPage"
    dynamodb = boto3.resource('dynamodb')
    config_table = dynamodb.Table(config_table_name)
    try:
        response = config_table.get_item(Key={'configKey': config_key})
    except ClientError as e:
        print(e.response['Error']['Message'])
        return None
    else:
        try:
            config_data = response['Item']["configValue"]
            # publicBuyers:
            # - cint
            # - lucid
            # buyer:
            # cint:
            # id:
            # name:
            # appId:
            # secretkey:
            # URL:
            # defaultCPI:
            # maxCPI:
            # parameters:
            # createdAt:
            # updatedAt:
            if buyer_name in config_data["buyer"].keys():
                buyer_object = config_data["buyer"][buyer_name]
                return buyer_object
        except Exception as e:
            print(e)
            return None
    return None


def take_survey(params):
    user_id = params["user_id"]
    buyer_name = params["buyer_name"]
    ip = params["ip"]
    survey_object = get_survey_object(buyer_name)
    default_cpi = survey_object["defaultCPI"]

    transaction_id = uuid.uuid1()
    started = datetime.utcnow().isoformat()

    try:
        data = {
            'TransactionId': str(transaction_id),
            "UserId": user_id,
            'status': "start",
            'CPI': default_cpi,
            'Payout': "NULL",
            'IP': ip,
            'Started': str(started),
            'Completed': "NULL",
            'Redirected': "NULL"
        }
        dynamodb = boto3.resource('dynamodb')
        transaction_table = dynamodb.Table(os.environ["TRANSACTION_TABLE"])
        response = transaction_table.put_item(
            Item=data
        )
        return data
    except Exception as e:
        print(f'Create Profile Failed: {e}')
    return None


def generate_entry_url(params, transaction_id):
    buyer_name = params["buyer_name"]
    survey = get_survey_object(buyer_name)
    if survey is None:
        return None
    entry_url = "{0}?si={1}&ssi={2}".format(survey['URL'], survey['appId'], transaction_id)
    return entry_url


# This lambda is called by API end point '/TakeSurvey'
# It is return redirect url for frontend.
def lambda_handler(event, context):
    params = event["queryStringParameters"]
    data = take_survey(params)
    if data is None:
        return {
            'statusCode': 400,
            'body': {"data": data}
        }
    entry_url = generate_entry_url(params, data["TransactionId"])
    if entry_url is None:
        return {
            'statusCode': 400,
            'body': {"entryURL": "fix"}
        }
    return {
        'statusCode': 200,
        'body': {"redirect": entry_url}
    }

