import boto3
from util import sudocoins_logger
from art.art import Art
import http.client
import json


log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    log.info(f'event: {event}')
    #input_json=event
    input_json = json.loads(event.get('body', '{}'))
    public_key = input_json['sub']

    collections = get_metamask_arts(public_key)

    return {
        "portfolio": collections
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_metamask_arts(public_address):
    if public_address.lower() == "0xe215e189d81bb81bc3a11d1b15ad556bb55e7645":
        collections = ["0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d:bored-ape-yacht-club","0x50f5474724e0ee42d9a4e711ccfb275809fd6d4a:the-sandbox", "0x7a3b97a7400e44dadd929431a3640e4fc47daebd:apes-in-space-nft", "0x364c828ee171616a39897688a831c2499ad972ec:sappy-seals","0xa3aee8bce55beea1951ef834b99f3ac60d1abeeb:veefriends"]
        key_list = []
        for i in collections:
            tmp = {
                "collection_id": i
            }
            key_list.append(tmp)

        if len(key_list) == 0:
            return []

        query = {
            'Keys': key_list,
            'ProjectionExpression': 'collection_id, preview_url, floor, median, maximum, collection_name, chart_data'
        }

        response = dynamodb.batch_get_item(RequestItems={'collections': query})

        collections = response['Responses']['collections']

        return collections
    else:
        try:
            path = "/api/v1/assets?owner="+public_address+"&order_direction=desc&offset=0&limit=50"
            log.info(f'path: {path}')
            conn = http.client.HTTPSConnection("api.opensea.io")
            api_key = {
                "X-API-KEY": "4714cd73a39041bf9cffda161163f8a5"
            }
            conn.request("GET", path, headers=api_key)
            response = conn.getresponse()
            response2 = response.read().decode('utf-8')
            open_sea_response = json.loads(response2)['assets']

            nfts = []
            nfts = nfts + open_sea_response

            try:
                count = 50
                while len(open_sea_response) >= 50:
                    path = "/api/v1/assets?owner=" + public_address + "&limit=50&offset=" + str(count)
                    conn = http.client.HTTPSConnection("api.opensea.io")
                    conn.request("GET", path)
                    response = conn.getresponse()
                    response2 = response.read().decode('utf-8')
                    open_sea_response = json.loads(response2)['assets']
                    count += 50
                    nfts = nfts + open_sea_response

                collections = []
                for i in nfts:
                    collection_address = i.get('asset_contract', {}).get('address', "unknown")
                    collection_name = i.get('collection', {}).get('name')
                    c_name = ("-".join(collection_name.split())).lower()
                    collection_code = collection_address + ":" + c_name
                    if collection_code in collections:
                        continue
                    else:
                        collections.append(collection_code)

                key_list = []
                for i in collections:
                    tmp = {
                        "collection_id": i
                    }
                    key_list.append(tmp)

                if len(key_list) == 0:
                    return []

                query = {
                    'Keys': key_list,
                    'ProjectionExpression': 'collection_id, preview_url, floor, median, maximum, collection_name, chart_data, collection_url'
                }

                response = dynamodb.batch_get_item(RequestItems={'collections': query})

                collections = response['Responses']['collections']
            except Exception as e:
                log.info(e)
                return []

        except Exception as e:
            log.info(e)
            return []

        return collections


