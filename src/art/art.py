import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import uuid
from util import sudocoins_logger
from art.art_history import ArtHistory

log = sudocoins_logger.get()


class Art:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb
        self.art_history = ArtHistory(self.dynamodb)

    def get_uploads(self, user_id):
        # returns the user's uploaded art sorted by timestamp
        art_uploads = self.dynamodb.Table('art_uploads').query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ScanIndexForward=False,
            IndexName='User_uploaded_art_view_idx',
            ExpressionAttributeNames={'#n': 'name'},
            ProjectionExpression="shareId, click_count, art_url, art_id,"
                                 "preview_url, #n")

        return art_uploads['Items']

    def get_arts(self, art_ids):
        # returns art records. Single or batch. Argument must be a list
        client = boto3.client('dynamodb')
        art_keys = []

        for i in art_ids:
            element = {'art_id': {'S': i}}

            art_keys.append(element)

        art_record = client.batch_get_item(
            RequestItems={
                'art': {
                    'Keys': art_keys,
                    'ExpressionAttributeNames': {
                        '#N': 'name'
                    },
                    'ProjectionExpression': 'art_id, click_count, art_url,'
                                            'recent_sk, preview_url, #N'
                }
            }
        )

        print(type(art_record['Responses']['art']))

        newlist = sorted(art_record['Responses']['art'], key=lambda k: int(k['click_count']['N']), reverse=True)

        return newlist

    def get_recent(self, count, timestamp):
        # returns recent art records paginated
        recent_art = self.dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("recent_sk").lt(timestamp),
            ScanIndexForward=False,
            Limit=count,
            IndexName='Recent_index',
            ProjectionExpression="art_id, preview_url, art_url, #n, click_count, recent_sk",
            ExpressionAttributeNames={'#n': 'name'})

        return recent_art

    def get_trending(self):
        # returns art sorted by click_count
        trending_art = self.dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true'),
            ScanIndexForward=False,
            Limit=250,
            IndexName='Trending-index',
            ProjectionExpression="art_id, click_count")

        return trending_art

    def register_click(self, data):
        # if a user's share url
        if 'shareId' in data:
            print("shareId:" + data['shareId'])
            # update view count for art in art_uploads
            self.dynamodb.Table('art_uploads').update_item(
                Key={'shareId': data['shareId']},
                UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':start': 0
                },
                ReturnValues="UPDATED_NEW"
            )
            print("art_uploads click_count increased")
            # get some data about this art in art_uploads
            art_uploads_record = self.dynamodb.Table('art_uploads').get_item(
                Key={'shareId': data['shareId']},
                ProjectionExpression="art_id, user_id")['Item']
            log.debug(f'art_uploads_record: {art_uploads_record}')
            if 'user_id' in art_uploads_record:
                self.update_user_count(art_uploads_record['user_id'])

            # get art record in art table and update click count
            if 'art_id' in art_uploads_record:
                art_id = art_uploads_record['art_id']
                self.dynamodb.Table('art').update_item(
                    Key={'art_id': art_id},
                    UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
                    ExpressionAttributeValues={
                        ':inc': 1,
                        ':start': 0
                    },
                    ReturnValues="UPDATED_NEW"
                )
                print("art record click count increased")

        # if it's not a custom art url, then it's a generic art url
        elif 'art_id' in data:
            # add to click count
            self.dynamodb.Table('art').update_item(
                Key={'art_id': data['art_id']},
                UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':start': 0
                },
                ReturnValues="UPDATED_NEW"
            )
            print("art record click count increased")
            # no need to add points to user profile

    def update_user_count(self, user_id):
        # update user profile click_count
        self.dynamodb.Table('Profile').update_item(
            Key={'userId': user_id},
            UpdateExpression="SET click_count = if_not_exists(click_count, :start) + :inc",
            ExpressionAttributeValues={
                ':inc': 1,
                ':start': 0
            },
            ReturnValues="UPDATED_NEW"
        )
        print("user profile click_count increased")
        # get some user data about click count
        profile_record = self.dynamodb.Table('Profile').get_item(
            Key={'userId': user_id},
            ProjectionExpression="click_count, click_count_paid")
        click_count = profile_record['Item']['click_count']
        if 'click_count_paid' in profile_record['Item']:
            click_count_paid = profile_record['Item']['click_count_paid']
        else:
            click_count_paid = 0
        # pay user if they earned sudocoins
        if click_count - click_count_paid > 100:
            self.dynamodb.Table('Profile').update_item(
                Key={'userId': user_id},
                UpdateExpression="SET click_count_paid = if_not_exists(click_count_paid, :start) + :inc, "
                                 "sudocoins = if_not_exists(sudocoins, :start) + :inc2",
                ExpressionAttributeValues={
                    ':inc': 100,
                    ':start': 0,
                    ':inc2': 10
                },
                ReturnValues="UPDATED_NEW"
            )
            self.addLedgerRecord(5, user_id, '100 Views')

    def share(self, user_id, art_id):

        time_now = str(datetime.utcnow().isoformat())

        art_record = self.dynamodb.Table('art').get_item(
            Key={'art_id': art_id},
            ProjectionExpression="art_id, preview_url, art_url, #n, click_count, buy_url, #tc,"
                                 "open_sea_data ",
            ExpressionAttributeNames={'#n': 'name', '#tc': 'contractId#tokenId'})

        if 'Item' in art_record:

            dedupe_key = str(user_id) + '#' + art_record['Item']['contractId#tokenId']
            art_uploads_Object = self.dynamodb.Table('art_uploads').query(
                KeyConditionExpression=Key("dedupe_key").eq(dedupe_key),
                IndexName='User_upload_dedupe_idx')
            if art_uploads_Object['Count'] > 0:
                msg = {
                    "shareId": art_uploads_Object['Items'][0]['shareId']
                }
                return msg
            else:
                shareId = str(uuid.uuid1())
                art_uploads_record = {
                    "shareId": shareId,
                    'contractId#tokenId': art_record['Item']['contractId#tokenId'],
                    "name": art_record['Item']['name'],
                    "buy_url": art_record['Item']['buy_url'],
                    "user_id": user_id,
                    'preview_url': art_record['Item']['preview_url'],
                    'art_url': art_record['Item']['art_url'],
                    "open_sea_data": art_record['Item']['open_sea_data'],
                    "click_count": 0,
                    "timestamp": time_now,
                    "dedupe_key": dedupe_key,
                    "art_id": art_id
                }
                self.dynamodb.Table('art_uploads').put_item(
                    Item=art_uploads_record
                )
                msg = {
                    "shareId": shareId
                }
                return msg
        else:
            art_uploads_record = self.dynamodb.Table('art_uploads').get_item(
                Key={'shareId': art_id},
                ProjectionExpression="art_id, user_id, shareId, #n,"
                                     "buy_url, preview_url, art_url, open_sea_data, #tc ",
                ExpressionAttributeNames={'#n': 'name', '#tc': 'contractId#tokenId'})['Item']
            if user_id == art_uploads_record['user_id']:
                msg = {
                    "shareId": art_uploads_record['shareId']
                }
                return msg
            else:
                shareId = str(uuid.uuid1())
                art_uploads_record = {
                    "shareId": shareId,
                    'contractId#tokenId': art_uploads_record['contractId#tokenId'],
                    "name": art_uploads_record['name'],
                    "buy_url": art_uploads_record['buy_url'],
                    "user_id": user_id,
                    'preview_url': art_uploads_record['preview_url'],
                    'art_url': art_uploads_record['art_url'],
                    "open_sea_data": art_uploads_record['open_sea_data'],
                    "click_count": 0,
                    "timestamp": time_now,
                    "dedupe_key": str(user_id) + '#' + art_uploads_record['contractId#tokenId'],
                    "art_id": art_id
                }
                self.dynamodb.Table('art_uploads').put_item(
                    Item=art_uploads_record
                )
                msg = {
                    "shareId": shareId
                }
                return msg

    def addLedgerRecord(self, amount, userId, type_value):
        ledgerTable = self.dynamodb.Table('Ledger')
        transactionId = str(uuid.uuid1())
        updated = str(datetime.utcnow().isoformat())

        ledgerTable.put_item(
            Item={
                'userId': userId,
                'transactionId': transactionId,
                'amount': amount,
                'status': 'Complete',
                'lastUpdate': updated,
                'type': type_value
            }
        )

        self.art_history.updateProfile(userId)

