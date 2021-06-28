import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    leaderboard = dynamodb.Table('Profile').query(
        KeyConditionExpression=Key("currency").eq('usd'),
        ScanIndexForward=False,
        Limit=5,
        IndexName='leaderboard',
        ProjectionExpression="userId, click_count, gravatarEmail, facebookUrl, email")

    leaders = leaderboard['Items']

    set_config(leaders)

    return {
        'leaderboard': leaders
    }


def set_config(leaders):
    config_table = dynamodb.Table('Config')

    updated_leaderboard = config_table.update_item(
        Key={
            'configKey': 'Leaderboard'
        },
        UpdateExpression="set leaders=:lead",
        ExpressionAttributeValues={
            ":lead": leaders
        },
        ReturnValues="ALL_NEW"
    )

    print(updated_leaderboard)

