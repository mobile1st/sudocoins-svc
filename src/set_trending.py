import boto3
from art import Art
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):

    art_uploads_record = art.get_arts(event['arts'])

    set_config(art_uploads_record)

    return {
        'arts': art_uploads_record
    }


def set_config(art_uploads_record):
    config_table = dynamodb.Table('Config')

    updated_art = config_table.update_item(
        Key={
            'configKey': 'HomePage'
        },
        UpdateExpression="set art=:art",
        ExpressionAttributeValues={
            ":art": art_uploads_record
        },
        ReturnValues="ALL_NEW"
    )

    print(updated_art)