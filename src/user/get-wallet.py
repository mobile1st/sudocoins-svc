import boto3
from util import sudocoins_logger
import http.client
import json
from decimal import Decimal, getcontext

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    log.info(f'event: {event}')
    public_key = event.get('queryStringParameters').get('ethAddress')

    configTable = dynamodb.Table('Config')
    configKey = "HomePage"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']

    rate = str(config['ethRate'])

    collections, eth_valuation = get_metamask_arts(public_key, rate)

    return {
        "nfts": collections,
        "eth_valuation": eth_valuation,
        "usd_valuation": eth_valuation/config['ethRate']
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_metamask_arts(public_address, rate):
    try:
        path = "/api/v1/assets?owner=" + public_address + \
            "&order_direction=desc&offset=0&limit=50"
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
                path = "/api/v1/assets?owner=" + public_address + \
                    "&limit=50&offset=" + str(count)
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
                collection_address = i.get(
                    'asset_contract', {}).get('address', "unknown")
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

            response = dynamodb.batch_get_item(
                RequestItems={'collections': query})

            response = response['Responses']['collections']

            eth_valuation = 0
            new_collections = []
            for i in response:
                i.update(collections[i['collection_id']])
                new_collections.append(i)
                # collections[i['collection_id']].update(i)

                try:
                    eth_valuation += Decimal(str(i['count'])) * \
                        Decimal(str(i['open_sea_stats']['floor_price']))
                    '''
                    valuation = {
                        "eth_valuation": valuation,
                        "usd_valuation": Decimal(valuation / rate)
                    }
                    collections[i['collection_id']].update(valuation)
                   '''

                except Exception as e:
                    log.info(e)
                    continue

        except Exception as e:
            log.info(e)
            return []

    except Exception as e:
        log.info(e)
        return []

    return new_collections, eth_valuation
