from decimal import Decimal
import boto3
import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import uuid
import sudocoins_logger

log = sudocoins_logger.get()


class Art:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def share(self, contractId, tokenId, open_sea_response, inputUrl, userId):
        time_now = str(datetime.utcnow().isoformat())

        open_sea = {
            'redirect': inputUrl,
            'name': open_sea_response['name'],
            'description': open_sea_response['description'],
            "image_url": open_sea_response['image_url'],
            "image_preview_url": open_sea_response['image_preview_url'],
            "image_thumbnail_url": open_sea_response['image_thumbnail_url'],
            "image_original_url": open_sea_response['image_original_url'],
            "animation_url": open_sea_response['animation_url'],
            "animation_original_url": open_sea_response['animation_original_url'],
        }

        # check to see if art already exists
        artObject = self.dynamodb.Table('art').get_item(
            Key={'contractId#tokenId': userId}
        )

        if 'Item' not in artObject:
            art_record = {
                'contractId#tokenId': str(contractId) + "#" + str(tokenId),
                "contractId": contractId,
                "tokenId": tokenId,
                "open_sea_data": open_sea,
                "timestamp": time_now,
                "recent_sk": time_now + "#" + str(uuid.uuid1()),
                "clicks": 0,
                "first_user": userId
            }
            self.dynamodb.Table('art').put_item(
                Item=art_record
            )

        art_uploads_record = {
            "shareId": str(uuid.uuid1()),
            'contractId#tokenId': str(contractId) + "#" + str(tokenId),
            "url": inputUrl,
            "user_id": userId,
            "open_sea_data": open_sea,
            "clicks": 0,
            "timestamp": time_now
        }
        self.dynamodb.Table('art_uploads').put_item(
            Item=art_uploads_record
        )

        newSudo = self.dynamodb.Table('Profile').update_item(
            Key={'userId': userId},
            UpdateExpression="SET sudocoins = if_not_exists(sudocoins, :start) + :inc",
            ExpressionAttributeValues={
                ':inc': 10,
                ':start': 0
            },
            ReturnValues="UPDATED_NEW"
        )

        sudo = newSudo['Attributes']['sudocoins']
        art_uploads_record['sudocoins'] = sudo

        return art_uploads_record
