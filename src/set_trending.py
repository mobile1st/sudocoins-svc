import boto3
from art import Art
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):

    trending_art = art.get_trending()

    #set_config(art_uploads_record)

    return {
        'trending': trending_art
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