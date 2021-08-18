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
            ProjectionExpression="art_id, preview_url, art_url, #n, click_count, mime_type, cdn_url, "
                                 "tags, last_sale_price, open_sea_data",
            ExpressionAttributeNames={'#n': 'name'})
        try:
            if 'Item' in art:
                art['Item']['alt'] = art['Item'].get('name', "") + " " \
                                     + art['Item'].get('open_sea_data', {}).get("description", "")
                if 'tags' in art['Item'] and isinstance(art['Item']['tags'], list):
                    for i in art['Item']['tags']:
                        tmp = " " + str(i)
                        art['Item']['alt'] += tmp
                del art['open_sea_data']
        except Exception as e:
            log.info(e)

        return self.__use_cdn_url(art['Item'] if art.get('Item') else None)

    def add(self, contract_token_id, art_url, preview_url, buy_url, open_sea, user_id, tags):
        time_now = str(datetime.utcnow().isoformat())
        art_id = str(uuid.uuid1())
        log.info(f"art.add {art_id} {open_sea}")
        creator_address = open_sea['creator'].get('address') if open_sea.get('creator') else "unknown"
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
            "creator": creator_address,
            "process_status": "STREAM_TO_S3",
            "tags": tags,
            "event_date": "0",
            "event_type": "manually added",
            "blockchain": "Ethereum",
            "last_sale_price": 0
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
            ProjectionExpression="art_id, preview_url, art_url, #n, click_count, recent_sk, mime_type, cdn_url, tags, last_sale_price",
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

        if self.__is_html(art):
            return art

        # # google does image sizing well
        # if self.__is_image(art) and 'googleusercontent.com/' in art['preview_url']:
        #     parts = art['preview_url'].split('=')
        #     art['art_url'] = f'{parts[0]}=s4096'
        #     del art['cdn_url']

        # we have a cdn too
        if 'cdn_url' in art:
            art['art_url'] = art['cdn_url']
            del art['cdn_url']

        return art

    def __is_html(self, art):
        art_url: str = art.get('art_url')
        mime_type: str = art.get('mime_type')
        if not art_url or '.googleusercontent.com/' in art_url:
            return False

        if mime_type:
            return mime_type.startswith('text/html')

        # no mimetype yet, make an educated guess
        extensions = [
            'jpg', 'jpeg', 'gif', 'svg', 'png', 'eps', 'webp', 'heic', 'bmp', 'tif', 'tiff', 'tga',
            'mp4', 'webm', 'mov', 'mkv',
            'mp3', 'wav', 'flac', 'ac3', 'ogg']
        for ext in extensions:
            if art_url.endswith('.'+ext):
                return False

        return True

    def __is_image(self, art):
        mime_type = art.get('mime_type')
        if mime_type and 'image' in mime_type:
            return True

        url = art.get('art_url')
        if '.jpg' in url or '.png' in url or '.gif' in url or '.svg' in url:
            return True

        return False

    def get_arts(self, art_ids):
        log.info(f"art.get_arts {art_ids}")
        if not art_ids or len(art_ids) == 0:
            return []

        art_index = {}
        art_keys = [{'art_id': i} for i in art_ids]
        for i in [art_keys[x:x + 100] for x in range(0, len(art_keys), 100)]:
            query = {
                'Keys': i,
                'ProjectionExpression': 'art_id, click_count, art_url, recent_sk, preview_url, #N, mime_type, cdn_url, tags, last_sale_price',
                'ExpressionAttributeNames': {'#N': 'name'}
            }
            response = self.dynamodb.batch_get_item(RequestItems={'art': query})
            for art in response['Responses']['art']:
                self.__use_cdn_url(art)
                art_index[art['art_id']] = art

        # preserve query art_id order
        result = []
        for art_id in art_ids:
            art = art_index.get(art_id)
            if art:
                result.append(art)

        return result


    def auto_add(self, contract_token_id, art_url, preview_url, buy_url, open_sea, art_object, eth_sale_price):
        time_now = str(datetime.utcnow().isoformat())
        art_id = str(uuid.uuid1())
        log.info(f"art.add {art_id} {open_sea} {art_object}")
        creator_address = open_sea['creator'].get('address') if open_sea.get('creator') else "unknown"
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
            "first_user": "ingest",
            "sort_idx": 'true',
            "creator": creator_address,
            "process_status": "STREAM_TO_S3",
            "tags": [],
            "event_date": art_object.get('created_date'),
            "event_type": art_object.get('event_type'),
            "blockchain": art_object.get('blockchain'),
            "last_sale_price": eth_sale_price,
            "collection_address": art_object.get('asset', {}).get('asset_contract', {}).get('address', "unknown"),
            "collection_data": {
                "name": art_object.get('asset', {}).get('collection', {}).get('name'),
                "image_url": art_object.get('asset', {}).get('collection', {}).get('image_url')
            }
        }

        if art_url == "" and preview_url is None:
            art_record["ingest_status"] = 0
        else:
            art_record["ingest_status"] = 1

        self.art_table.put_item(Item=art_record)
        try:
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
        except Exception as e:
            print(e)

        return art_record
