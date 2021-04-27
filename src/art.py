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
            Key={'contractId#tokenId': str(contractId) + "#" + str(tokenId)}
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

        # check to see if art_uploads record already exists
        art_uploads_Object = self.dynamodb.Table('art_uploads').query(
            KeyConditionExpression=Key("userId").eq(userId) & Key("User_upload_dedupe_idx").eq(str(contractId) + "#" + str(tokenId)),
            IndexName='userId-started-index')

        if art_uploads_Object['Count'] > 0:
            msg = {
                "message": "Art already added to Gallery",
                "shareId": art_uploads_Object['Items'][0]['shareId']
            }

            return msg

        else:
            art_uploads_record = {
                "shareId": str(uuid.uuid1()),
                'contractId#tokenId': str(contractId) + "#" + str(tokenId),
                "url": inputUrl,
                "user_id": userId,
                "open_sea_data": open_sea,
                "click_count": 0,
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

    def get_user_uploaded_art_view(self, user_id):
        # returns the user's uploaded art sorted by timestamp

        return

    def get_art_uploads(self, shareId):
        # returns the art_uploads_record based on shareId
        art_uploads_record = self.dynamodb.Table('art').get_item(
            Key={'shareId': shareId}
        )

        if 'Item' in art_uploads_record:
            return art_uploads_record['Item']

        else:
            return {
                "message": "Art doesn't exist in Gallery based on shareId"
            }

    def get_art(self, contractTokenId):
        # returns the art_uploads_record based on shareId
        art_record = self.dynamodb.Table('art').get_item(
            Key={'contractId#tokenId': contractTokenId}
        )

        if 'Item' in art_record:
            return art_record['Item']

        else:
            return {
                "message": "Art doesn't exist in Gallery based on contractId#tokenId"
            }
