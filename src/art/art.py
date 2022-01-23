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
        dynamodb = boto3.resource('dynamodb')
        log.info(f"art.get {art_id}")
        art = self.art_table.get_item(
            Key={'art_id': art_id},
            ProjectionExpression="art_id, preview_url, art_url, #n, mime_type, cdn_url, "
                                 "last_sale_price, list_price, description, collection_id, collection_data, "
                                 "collection_name, #T, blockchain, buy_url",
            ExpressionAttributeNames={'#n': 'name', '#T': 'contractId#tokenId'})
        try:
            if 'Item' in art:
                if 'name' in art['Item'] and art['Item']['name'] is None:
                    name = art['Item'].get('collection_data', {}).get('name', "")
                    number = art['Item'].get("contractId#tokenId", "")
                    number = number.split('#')[1]
                    art['Item']['name'] = name + " #" + str(number)
                    del art['Item']['contractId#tokenId']
            else:
                art = dynamodb.Table('art').query(
                    KeyConditionExpression=Key("collection_item_url").eq(art_id),
                    IndexName='collection_item_url-index',
                    ProjectionExpression="art_id, preview_url, art_url, #n, mime_type, cdn_url, "
                                         "last_sale_price, list_price, description, collection_id, collection_data, "
                                         "collection_name, #T, blockchain, buy_url",
                    ExpressionAttributeNames={'#n': 'name', '#T': 'contractId#tokenId'})

                if not art.get('Items'):
                    return None

                art = art.get('Items')[0]
                if 'name' in art and art['name'] is None:
                    name = art.get('collection_data', {}).get('name', "")
                    number = art.get("contractId#tokenId", "")
                    number = number.split('#')[1]
                    art['name'] = name + " #" + str(number)
                    del art['contractId#tokenId']

                return self.__use_cdn_url(art)


        except Exception as e:
            log.info(e)

        return self.__use_cdn_url(art['Item'] if art.get('Item') else None)


    def get_recent(self, count, timestamp):
        log.info(f"art.get_recent {count} {timestamp}")
        res = self.art_table.query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("recent_sk").lt(timestamp),
            ScanIndexForward=False,
            Limit=count,
            IndexName='Recent_index',
            ProjectionExpression="art_id, preview_url, art_url, #n, click_count, recent_sk, mime_type, cdn_url, last_sale_price, collection_data, collection_address, open_sea_data.description, description, collection_id, blockchain",
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
        '''
        if 'cdn_url' in art:
            art['art_url'] = art['cdn_url']
            del art['cdn_url']
        '''

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
                'ProjectionExpression': 'art_id, click_count, art_url, recent_sk, preview_url, #N, mime_type, cdn_url, last_sale_price, #T, collection_data, collection_address, open_sea_data.description, description, collection_id, blockchain',
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



