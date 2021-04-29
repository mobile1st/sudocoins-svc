import boto3
from art import Art
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    # need to make sure list doesn't contain duplicates or the batch function will break

    # demo for now
    arts = ["1c29eb04-a7d9-11eb-ac30-e747e605e5c2",
            "3826e924-a7e0-11eb-baa2-496c13a4ff8e",
            "442a1d67-a7db-11eb-8334-671d4f5dcf35",
            "a696cd34-a8a9-11eb-842a-fd6368dd0d69",
            "1c29eb04-a7d9-11eb-ac30-e747e605e5c2",
            "3826e924-a7e0-11eb-baa2-496c13a4ff8e",
            "442a1d67-a7db-11eb-8334-671d4f5dcf35",
            "a696cd34-a8a9-11eb-842a-fd6368dd0d69"]

    art_uploads_record = art.get_arts(arts)

    set_config(art_uploads_record)

    return {
        'statusCode': 200,
        'body': art_uploads_record
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