import boto3
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime
import uuid

from util import sudocoins_logger

log = sudocoins_logger.get()


class Art:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb
        self.art_table = self.dynamodb.Table('art')
        self.sns = boto3.client("sns")
        self.dynamodb_client = boto3.client('dynamodb')

    def get_id(self, contract_token_id):
        log.info(f"art.get_id {contract_token_id}")
        art_object = self.art_table.query(
            KeyConditionExpression=Key('contractId#tokenId').eq(contract_token_id),
            ScanIndexForward=False,
            IndexName='Art_dedupe_idx')
        return art_object['Items'][0]['art_id'] if art_object['Count'] > 0 else None

    def get(self, art_id):
        log.info(f"art.get {art_id}")
        art = self.art_table.get_item(
            Key={'art_id': art_id},
            ProjectionExpression="art_id, preview_url, art_url, #n, click_count, file_type, cdn_url",
            ExpressionAttributeNames={'#n': 'name'})
        return self.__use_cdn_url(art['Item'] if art.get('Item') else None)

    def add(self, contract_token_id, art_url, preview_url, buy_url, open_sea, user_id):
        time_now = str(datetime.utcnow().isoformat())
        art_id = str(uuid.uuid1())
        log.info(f"art.add {art_id}")
        art_record = {
            'art_id': art_id,
            "name": open_sea['name'],
            'buy_url': buy_url,
            'contractId#tokenId': contract_token_id,
            'preview_url': preview_url,
            'art_url': art_url,
            "open_sea_data": open_sea,
            "timestamp": time_now,
            "recent_sk": time_now + "#" + art_id,
            "click_count": 0,
            "first_user": user_id,
            "sort_idx": 'true',
            "creator": open_sea['creator'],
            "process_status": "STREAM_TO_S3"
        }

        self.art_table.put_item(Item=art_record)
        self.sns.publish(
            TopicArn='arn:aws:sns:us-west-2:977566059069:ArtProcessor',
            MessageStructure='string',
            MessageAttributes={
                'art_id': {
                    'DataType': 'String',
                    'StringValue': art_id
                },
                'art_url': {
                    'DataType': 'String',
                    'StringValue': art_url
                },
                'process': {
                    'DataType': 'String',
                    'StringValue': "STREAM_TO_S3"
                }
            },
            Message=json.dumps(art_record)
        )
        return art_record

    def get_recent(self, count, timestamp):
        log.info(f"art.get_recent {count} {timestamp}")
        res = self.art_table.query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("recent_sk").lt(timestamp),
            ScanIndexForward=False,
            Limit=count,
            IndexName='Recent_index',
            ProjectionExpression="art_id, preview_url, art_url, #n, click_count, recent_sk, file_type, cdn_url",
            ExpressionAttributeNames={'#n': 'name'}
        )
        if not res.get('Items'):
            return None

        for art in res['Items']:
            self.__use_cdn_url(art)

        return res['Items']

    def __use_cdn_url(self, art):
        if not art:
            return art
        if 'cdn_url' in art:
            art['art_url'] = art['cdn_url']
            del art['cdn_url']
        return art

    def get_arts(self, art_ids):
        log.info(f"art.get_arts {art_ids}")

        art_keys = [{'art_id': {'S': i}} for i in art_ids]
        art_record = self.dynamodb_client.batch_get_item(
            RequestItems={
                'art': {
                    'Keys': art_keys,
                    'ExpressionAttributeNames': {'#N': 'name'},
                    'ProjectionExpression': 'art_id,click_count,art_url,recent_sk,preview_url,#N,file_type,cdn_url'
                }
            }
        )

        for art in art_record['Responses']['art']:
            if 'cdn_url' in art:
                art['art_url']['S'] = art['cdn_url']['S']
                del art['cdn_url']

        return sorted(art_record['Responses']['art'], key=lambda k: int(k['click_count']['N']), reverse=True)


a = Art(boto3.resource('dynamodb'))
print(a.get_arts(['13eba980-e0c9-11eb-a0a7-85584701e4ec', '89692549-e0c8-11eb-b213-85584701e4ec']))