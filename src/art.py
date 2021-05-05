import boto3
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
        contractTokenId = str(contractId) + "#" + str(tokenId)

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

        art_object = self.dynamodb.Table('art').query(
            KeyConditionExpression=Key('contractId#tokenId').eq(contractTokenId),
            ScanIndexForward=False,
            IndexName='Art_dedupe_idx')

        if not art_object['Count'] > 0:
            art_id = str(uuid.uuid1())
            art_record = {
                'art_id': art_id,
                'contractId#tokenId': contractTokenId,
                "open_sea_data": open_sea,
                "timestamp": time_now,
                "recent_sk": time_now + "#" + art_id,
                "click_count": 0,
                "first_user": userId,
                "sort_idx": 'true'
            }
            self.dynamodb.Table('art').put_item(
                Item=art_record
            )
        elif art_object['Count'] > 0:
            art_id = art_object['Items'][0]['art_id']

        # check to see if art_uploads record already exists

        dedupe_key = str(userId) + '#' + str(contractId) + "#" + str(tokenId)

        art_uploads_Object = self.dynamodb.Table('art_uploads').query(
            KeyConditionExpression=Key("dedupe_key").eq(dedupe_key),
            IndexName='User_upload_dedupe_idx')

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
                "timestamp": time_now,
                "dedupe_key": dedupe_key,
                "art_id": art_id
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

    def get_uploads(self, user_id):
        # returns the user's uploaded art sorted by timestamp
        art_uploads = self.dynamodb.Table('art_uploads').query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ScanIndexForward=False,
            IndexName='User_uploaded_art_view_idx',
            ExpressionAttributeNames={'#ct': 'contractId#tokenId', '#t': 'timestamp', "#ul": 'url'},
            ProjectionExpression="shareId, click_count, #ul,"
                                 "#ct, open_sea_data, #t")

        return art_uploads['Items']

    def get_by_share_id(self, shareId):
        # returns the art_uploads record based on shareId
        art_uploads_record = self.dynamodb.Table('art_uploads').get_item(
            Key={'shareId': shareId}
        )

        if 'Item' in art_uploads_record:
            return art_uploads_record['Item']

        else:
            return {
                "message": "Art doesn't exist in Gallery based on shareId"
            }

    def get_arts(self, art_ids):
        # returns art records. Single or batch. Argument must be a list
        client = boto3.client('dynamodb')
        art_keys = []

        for i in art_ids:
            element = {'art_id': {'S': i}}

            print(type(element))

            art_keys.append(element)

        print(art_keys)
        art_record = client.batch_get_item(
            RequestItems={
                'art': {
                    'Keys': art_keys,
                    'ProjectionExpression': 'art_id, open_sea_data, click_count, recent_sk'
                }
            }
        )

        return art_record['Responses']['art']

    def get_recent(self, count, timestamp):
        # returns recent art records paginated
        recent_art = self.dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("recent_sk").lt(timestamp),
            ScanIndexForward=False,
            Limit=count,
            IndexName='Recent_index',
            ProjectionExpression="#t, click_count, recent_sk,"
                                 "open_sea_data, #ct, art_id",
            ExpressionAttributeNames={'#ct': 'contractId#tokenId', '#t': 'timestamp'})

        return recent_art

    def get_trending(self):
        # returns art sorted by click_count
        trending_art = self.dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true'),
            ScanIndexForward=False,
            IndexName='Trending-index',
            ProjectionExpression="art_id, click_count")

        return trending_art
