import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
from collections import OrderedDict
from operator import getitem

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    last_day = (datetime.utcnow() - timedelta(days=14)).isoformat()

    vote_counts = get_votes(last_day)
    view_counts = get_views(last_day)
    buy_counts = get_buys(last_day)
    log.info("scores created")

    scores = merge_arts(vote_counts, view_counts, buy_counts)
    log.info("scores merged")

    creators = get_creators(scores)
    log.info("creator data received")
    log.info(creators)

    top_creators = creator_ranking(scores, creators)
    log.info("top creators list mapping finished")

    set_config(top_creators)
    log.info("config set")

    return top_creators


def creator_ranking(scores, creators):
    creator_data = {}

    for k in creators:
        for i in k:
            try:
                creator = i['open_sea_data']['M']['creator']['M']['address']['S']
                score = scores[i['art_id']['S']]

                if creator in creator_data:
                    creator_data[creator]['score'] += score
                else:
                    creator_data[creator] = {
                        "score": score,
                        "data": i['open_sea_data']['M']['creator']['M']
                    }
            except Exception as e:
                log.info(e)
                pass

    sorted_dict = OrderedDict(sorted(creator_data.items(), key=lambda x: getitem(x[1], 'score'), reverse=True))

    top_20 = list(sorted_dict.items())[:20]

    return top_20


def get_creators(scores):
    client = boto3.client('dynamodb')
    art_keys = []
    for i in scores:
        element = {'art_id': {'S': i}}
        art_keys.append(element)

    chunks = [art_keys[x:x + 100] for x in range(0, len(art_keys), 100)]

    creators = []
    for i in chunks:
        art_row = client.batch_get_item(
            RequestItems={
                'art': {
                    'Keys': i,
                    'ProjectionExpression': 'art_id, open_sea_data'
                }
            }
        )
        sub_creators = art_row['Responses']['art']
        creators.append(sub_creators)

    return creators


def get_votes(last_day):
    votes = dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("type").eq('vote') & Key("timestamp").gt(last_day),
        ScanIndexForward=False,
        IndexName='type-timestamp-index',
        ProjectionExpression="art_id, vote"
    )['Items']
    vote_counts = {}
    for i in votes:
        if i['art_id'] in vote_counts:
            vote_counts[i['art_id']] += 1
        else:
            vote_counts[i['art_id']] = 1

    log.info(len(vote_counts))

    return vote_counts


def get_views(last_day):
    views = dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("type").eq('view') & Key("timestamp").gt(last_day),
        ScanIndexForward=False,
        IndexName='type-timestamp-index',
        ProjectionExpression="art_id, vote"
    )['Items']
    # create a dictionary that maps art_id=>view_count int
    view_counts = {}
    for i in views:
        if i['art_id'] in view_counts:
            view_counts[i['art_id']] += 1
        else:
            view_counts[i['art_id']] = 1

    log.info(len(view_counts))

    return view_counts


def get_buys(last_day):
    buys = dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("type").eq('buy') & Key("timestamp").gt(last_day),
        ScanIndexForward=False,
        IndexName='type-timestamp-index',
        ProjectionExpression="art_id, vote"
    )['Items']
    buy_counts = {}
    # art_id=>buy_count int
    for i in buys:
        if i['art_id'] in buy_counts:
            buy_counts[i['art_id']] += 1
        else:
            buy_counts[i['art_id']] = 1

    log.info(len(buy_counts))

    return buy_counts


def merge_arts(vote_counts, view_counts, buy_counts):
    scores = {}
    for i in vote_counts:
        if i in scores:
            scores[i] += vote_counts[i]
        else:
            scores[i] = vote_counts[i]
    for i in view_counts:
        if i in scores:
            scores[i] += view_counts[i]
        else:
            scores[i] = view_counts[i]
    for i in buy_counts:
        if i in scores:
            scores[i] += buy_counts[i]
        else:
            scores[i] = buy_counts[i]

    log.info(len(scores))

    return scores


def set_config(top_creators):
    config_table = dynamodb.Table('Config')

    updated_leaderboard = config_table.update_item(
        Key={
            'configKey': 'Leaderboard'
        },
        UpdateExpression="set creators=:create",
        ExpressionAttributeValues={
            ":create": top_creators
        },
        ReturnValues="ALL_NEW"
    )

    return



