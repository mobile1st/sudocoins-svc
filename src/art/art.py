import boto3
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime
import uuid
import pymysql
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
            ProjectionExpression="art_id, preview_url, art_url, #n, mime_type, cdn_url, "
                                 "last_sale_price, list_price, description, collection_id, collection_data, "
                                 "collection_name, #T",
            ExpressionAttributeNames={'#n': 'name', '#T': 'contractId#tokenId'})
        try:
            if 'Item' in art:
                if 'name' in art['Item'] and art['Item']['name'] is None:
                    name = art['Item'].get('collection_data', {}).get('name', "")
                    number = art['Item'].get("contractId#tokenId", "")
                    number = number.split('#')[1]
                    art['Item']['name'] = name + " #" + str(number)
                    del art['Item']['contractId#tokenId']

        except Exception as e:
            log.info(e)

        return self.__use_cdn_url(art['Item'] if art.get('Item') else None)

    def add(self, contract_token_id, art_url, preview_url, buy_url, open_sea, user_id):
        time_now = str(datetime.utcnow().isoformat())
        art_id = str(uuid.uuid1())
        log.info(f"art.add {art_id} {open_sea}")
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
            "process_status": "STREAM_TO_S3",
            "event_date": "0",
            "event_type": "manually added",
            "blockchain": "Ethereum",
            "last_sale_price": 0,
            "process_to_google_search": "TO_BE_INDEXED"
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
            ProjectionExpression="art_id, preview_url, art_url, #n, click_count, recent_sk, mime_type, cdn_url, last_sale_price, collection_data, collection_address, open_sea_data.description, description, collection_id",
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
                'ProjectionExpression': 'art_id, click_count, art_url, recent_sk, preview_url, #N, mime_type, cdn_url, last_sale_price, #T, collection_data, collection_address, open_sea_data.description, description, collection_id',
                'ExpressionAttributeNames': {'#N': 'name', '#T': 'contractId#tokenId'}
            }
            response = self.dynamodb.batch_get_item(RequestItems={'art': query})
            for art in response['Responses']['art']:
                self.__use_cdn_url(art)
                if 'name' in art and art['name'] is not None:
                    if art['name'].find('Fragments of an Infinite Field') != -1:
                         art['art_url'] = art['preview_url']
                if 'name' in art and art['name'] is None:
                    name = art.get('collection_data', {}).get('name', "")
                    number = art.get("contractId#tokenId", "")
                    number = number.split('#')[1]
                    art['name'] = name + " #" + str(number)
                    del art['contractId#tokenId']
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
            "process_status": "STREAM_TO_S3",
            "event_date": art_object.get('created_date'),
            "event_type": art_object.get('event_type'),
            "blockchain": art_object.get('blockchain'),
            "last_sale_price": eth_sale_price,
            "collection_address": art_object.get('asset', {}).get('asset_contract', {}).get('address', "unknown"),
            "collection_data": {
                "name": art_object.get('asset', {}).get('collection', {}).get('name'),
                "image_url": art_object.get('asset', {}).get('collection', {}).get('image_url'),
                "description": art_object.get('asset', {}).get('collection', {}).get('description', "")
            },
            "process_to_google_search": "TO_BE_INDEXED",
            "collection_name": art_object.get('asset', {}).get('collection', {}).get('name'),
            "owner": art_object.get("owner", "unknown"),
            "seller": art_object.get("seller", "unknown")
        }

        if art_record['collection_name'] is not None and art_record['collection_address'] is not None:
            c_name = ("-".join(art_record['collection_name'].split())).lower()
            art_record['collection_id'] = art_record['collection_address'] + ":" + c_name
        else:
            art_record['collection_id'] = art_record['collection_address']

        log.info(f"art.add {art_record}")
        if art_record['preview_url'] is None:
            art_record['preview_url'] = art_record['art_url']

        if 'name' in art_record and art_record['name'] is None:
            name = art_record.get('collection_data', {}).get('name', "")
            number = art_record.get("contractId#tokenId", "")
            number = number.split('#')[1]
            art_record['name'] = name + " #" + str(number)

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
            log.info(f"open_sea {open_sea}")
            log.info(e)

        try:
            msg = {
                "collection_id": art_record.get('collection_id', "")
            }
            self.sns.publish(
                TopicArn='arn:aws:sns:us-west-2:977566059069:AddSearchTopic',
                MessageStructure='string',
                Message=json.dumps(msg)
            )
            log.info(f"add search message published")
        except Exception as e:
            log.info(e)

        try:
            msg = {
                "event_date": art_object.get('created_date'),
                "last_sale_price": eth_sale_price,
                "collection_id": art_record['collection_id'],
                'art_id': art_id,
                'contractId#tokenId': contract_token_id
            }
            self.sns.publish(
                TopicArn='arn:aws:sns:us-west-2:977566059069:AddTimeSeriesTopic',
                MessageStructure='string',
                Message=json.dumps(msg)
            )
            log.info(f"add time series published")
        except Exception as e:
            log.info(e)

        try:
            dynamodb = boto3.resource('dynamodb')
            dynamodb.Table('collections').update_item(
                Key={
                    'collection_id': art_record['collection_id']
                },
                UpdateExpression="SET sale_count = if_not_exists(sale_count, :start) + :inc, sales_volume = if_not_exists(sales_volume, :start2) + :inc2,"
                                 "collection_name = :cn, preview_url = :purl, collection_address = :ca, collection_date=:cd, sort_idx=:si",
                ExpressionAttributeValues={
                    ':start': 0,
                    ':inc': 1,
                    ':start2': 0,
                    ':inc2': art_record['last_sale_price'],
                    ':cn': art_record['collection_name'],
                    ':purl': art_record['preview_url'],
                    ':ca': art_record['collection_address'],
                    ':cd': art_object.get('collection_date', "0"),
                    ":si": "true"
                },
                ReturnValues="UPDATED_NEW"
            )
            log.info("collection table updated")
        except Exception as e:
            log.info(e)

        try:
            rds_host = "rds-proxy.proxy-ccnnpquqy2qq.us-west-2.rds.amazonaws.com"
            name = "admin"
            password = "RHV2CiqtjiZpsM11"
            db_name = "nft_events"

            conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=5)
            with conn.cursor() as cur:
                art_id = art_record['art_id']
                price = art_record['last_sale_price']
                collection_id = art_record['collection_id']
                collection_name = art_record['collection_name']
                contract_token = art_record['contractId#tokenId']
                event_date = art_record['event_date']
                time = time_now
                blockchain = art_record['blockchain']
                event_type = 'successful'
                row_values = (art_id, price, collection_id, collection_name, contract_token, event_date, time, blockchain, event_type)
                cur.execute(
                    'INSERT INTO `nft_events`.`open_sea_events` (`art_id`, `price`, `collection_id`, `collection_name`,`contract_token_id`, `event_date`, `created_date`, `blockchain`, `event_type`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    row_values)
                conn.commit()

        except Exception as e:
            log.info(e)


        return art_record

    def get_minted(self, count, timestamp):
        log.info(f"art.get_minted {count} {timestamp}")
        res = self.art_table.query(
            KeyConditionExpression=Key("event_type").eq('mint') & Key("recent_sk").lt(timestamp),
            ScanIndexForward=False,
            Limit=count,
            IndexName='event_type-recent_sk-index',
            ProjectionExpression="art_id, preview_url, art_url, #n, click_count, recent_sk, mime_type, cdn_url, last_sale_price, list_price, description, collection_id, #T, collection_name",
            ExpressionAttributeNames={'#n': 'name', '#T': 'contractId#tokenId'}
        )
        if not res.get('Items'):
            return None

        for art in res['Items']:
            self.__use_cdn_url(art)

        return res['Items']


