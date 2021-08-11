import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
from collections import OrderedDict
from operator import getitem

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    period = (datetime.utcnow() - timedelta(days=1)).isoformat()
    trending_art = get_trending(period)
    arts = []
    for i in trending_art:
        arts.append(i[0])

    set_config(arts)

    return {
        'trending': arts[:100]
    }


def get_trending(period):

    record = dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("recent_sk").gt(period),
        IndexName='top-sales',
        ProjectionExpression="art_id, last_sale_price"
    )

    data = record['Items']
    while 'LastEvaluatedKey' in record:
        record = dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("recent_sk").gt(period),
            IndexName='top-sales',
            ProjectionExpression="art_id, last_sale_price",
            ExclusiveStartKey=record['LastEvaluatedKey']
        )
        data.extend(record['Items'])

    sorted_arts = sorted(data.items(), key=lambda item: item[1], reverse=True)

    return sorted_arts



def set_config(arts):
    config_table = dynamodb.Table('Config')
    updated_art = config_table.update_item(
        Key={
            'configKey': 'TrendingArt'
        },
        UpdateExpression="set art=:art",
        ExpressionAttributeValues={
            ":art": arts
        },
        ReturnValues="ALL_NEW"
    )
    log.info(f'updated_art {updated_art}')

'''
def get_trending():
    last_day = (datetime.utcnow() - timedelta(days=7)).isoformat()

    vote_counts = get_votes(last_day)
    view_counts = get_views(last_day)
    buy_counts = get_buys(last_day)
    log.info("scores created")
    scores = merge_arts(vote_counts, view_counts, buy_counts)
    log.info("scores merged")

    print(scores)

    sorted_scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)

    return sorted_scores


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

    return scores
'''