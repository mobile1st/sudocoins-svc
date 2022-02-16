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
    public_key = event.get('queryStringParameters').get('ethAddress')

    collections = get_metamask_arts(public_key)

    return {
        "wallet": collections
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_metamask_arts(public_address):
    try:
        path = "/api/v1/assets?owner=" + public_address + "&order_direction=desc&offset=0&limit=50"
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

            collections = {}
            key_list = []
            for i in nfts:
                collection_address = i.get('asset_contract', {}).get('address', "unknown")
                collection_name = i.get('collection', {}).get('name')
                c_name = ("-".join(collection_name.split())).lower()
                collection_code = collection_address + ":" + c_name
                if collection_code in collections:
                    collections[collection_code]['count'] += 1
                    continue
                else:
                    collections[collection_code] = {}
                    collections[collection_code]['count'] = 1
                    key_list.append({"collection_id": collection_code})

            if len(key_list) == 0:
                return []

            query = {
                'Keys': key_list,
                'ProjectionExpression': 'collection_id, preview_url, collection_name, collection_url, open_sea_stats, more_charts'
            }

            response = dynamodb.batch_get_item(RequestItems={'collections': query})

            response = response['Responses']['collections']

            valuation = 0
            for i in response:
                collections[i['collection_id']].update(i)
                try:
                    valuation += collections[i['collection_id']]['count'] * \
                                 collections[i['collection_id']]['open_sea_stats']['floor_price']

                    valuation = {"eth_valuation": valuation}
                    collections[i['collection_id']].update(valuation)

                except Exception as e:
                    log.info(e)
                    continue

        except Exception as e:
            log.info(e)
            return []

    except Exception as e:
        log.info(e)
        return []

    return collections, valuation
