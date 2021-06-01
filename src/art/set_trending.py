import boto3
from art.art import Art
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    trending_art = art.get_trending()

    art_elements = trending_art['Items']

    arts = []

    for i in art_elements:
        arts.append(i['art_id'])

    set_config(arts)

    return {
        'trending': arts
    }


def set_config(arts):
    config_table = dynamodb.Table('Config')

    updated_art = config_table.update_item(
        Key={
            'configKey': 'TrendingArt'
        },
        UpdateExpression="set art=:art",
        ExpressionAttributeValues={
            ":art": arts
        },
        ReturnValues="ALL_NEW"
    )

    print(updated_art)
