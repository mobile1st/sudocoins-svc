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

    def get_id(self, contract_token_id):
        art_object = self.art_table.query(
            KeyConditionExpression=Key('contractId#tokenId').eq(contract_token_id),
            ScanIndexForward=False,
            IndexName='Art_dedupe_idx')
        return art_object['Items'][0]['art_id'] if art_object['Count'] > 0 else None

    def add(self, contract_token_id, art_url, preview_url, buy_url, open_sea, user_id):
        time_now = str(datetime.utcnow().isoformat())
        art_id = str(uuid.uuid1())
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
        log.info("art pushed to sns")
        return art_record
