import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
from operator import getitem

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    last_day = (datetime.utcnow() - timedelta(days=14)).isoformat()

    creators = get_creators(last_day)
    leaders = get_leaders(last_day)
    trending = get_trending()
    # log.info(f'leaders: {leaders} creators: {creators} trending: {trending}')

    set_config(leaders, creators, trending)
    # log.info("Leaderboard config set")


def get_trending():
    config_table = dynamodb.Table('Config')
    trending_arts = config_table.get_item(
        Key={'configKey': 'TrendingArt'},
        ProjectionExpression="art"
    )['Item']['art'][:40]

    trending = dynamodb.batch_get_item(
        RequestItems={
            'art': {
                'Keys': [{'art_id': i} for i in trending_arts],
                'ProjectionExpression': 'art_id, preview_url, #N, click_count, last_sale_price, art_url, collection_data',
                'ExpressionAttributeNames': {'#N': 'name'}
            }
        }
    )['Responses']['art']

    final_trending = []
    for i in trending_arts:
        for k in trending:
            if i == k['art_id'] and k['art_url'] != "":
                if k['name'] is None:
                    k['name'] = k['collection_data']['name']
                final_trending.append(k)

    return final_trending[:20]


def set_config(leaders, creators, trending):
    config_table = dynamodb.Table('Config')
    config_table.update_item(
        Key={'configKey': 'Leaderboard'},
        UpdateExpression="set leaders=:lead, creators=:create, trending=:trend",
        ExpressionAttributeValues={
            ":lead": leaders,
            ":create": creators,
            ":trend": trending
        }
    )


def get_leaders(last_day):
    scores = get_top20_view_leaders(last_day)
    # log.info(f'top20 leaders: {scores}')

    leaders = dynamodb.batch_get_item(
        RequestItems={
            'Profile': {
                'Keys': [{'userId': i[0]} for i in scores],
                'ProjectionExpression': 'userId, email, user_name, twitter_handle, gravatarEmail'
            }
        }
    )['Responses']['Profile']

    profiles = list(leaders)
    scores = dict(scores)
    for i in profiles:
        i['click_count'] = scores[i['userId']]['score']

    arts = []
    arts_dict = {}
    for i in scores:
        if scores[i]['art_id'] not in arts_dict:
            element = {'art_id': scores[i]['art_id']}
            arts.append(element)
            arts_dict[scores[i]['art_id']] = 0

    art_rows = dynamodb.batch_get_item(
        RequestItems={
            'art': {
                'Keys': arts,
                'ProjectionExpression': 'art_id, preview_url'
            }
        }
    )
    avatars = art_rows['Responses']['art']

    art_keys = {}
    for i in avatars:
        art_keys[i['art_id']] = i['preview_url']

    for i in scores:
        preview_url = art_keys[scores[i]['art_id']]
        for k in leaders:
            # print(k)
            if i == k['userId']:
                k['avatar'] = preview_url

    return sorted(leaders, key=lambda k: k['click_count'], reverse=True)


def get_top20_view_leaders(last_day):
    views = dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("type").eq('view') & Key("timestamp").gt(last_day),
        ScanIndexForward=False,
        FilterExpression="attribute_exists(influencer)",
        IndexName='type-timestamp-index',
        ProjectionExpression="art_id, influencer"
    )['Items']
    # create a dictionary that maps art_id=>view_count int
    view_counts = {}
    for art_id, influencer in [[i['art_id'], i['influencer']] for i in views]:
        if influencer in view_counts:
            if art_id in view_counts[influencer]:
                view_counts[influencer][art_id] += 1
                view_counts[influencer]['total'] += 1
            else:
                view_counts[influencer][art_id] = 1
                view_counts[influencer]['total'] += 1
        else:
            view_counts[influencer] = {art_id: 1}
            view_counts[influencer]['total'] = 1

    scores = {}
    for i in view_counts:
        scores[i] = {'score': view_counts[i]['total']}
        arts = view_counts[i]
        arts = {k: v for k, v in sorted(arts.items(), key=lambda item: item[1], reverse=True)}
        del arts['total']
        b = list(arts.keys())[0]
        scores[i]['art_id'] = b

    return sorted(scores.items(), key=lambda x: getitem(x[1], 'score'), reverse=True)[:20]


def get_creators(last_day):
    scores = {}
    add_votes(scores, last_day)
    add_views(scores, last_day)
    add_buys(scores, last_day)
    log.info("scores created")

    creators = get_creators_for_art_ids(scores)
    log.info("creator data received")

    return creator_ranking(scores, creators)


def creator_ranking(scores, creators):
    creator_data = {}
    for i in creators:
        creator = i.get('open_sea_data', {}).get('creator')
        if not creator:
            log.info(f'No creator for art_id: {i["art_id"]}')
            continue

        try:
            creator_address = creator['address']
            score = scores[i['art_id']]

            if creator_address in creator_data:
                creator_data[creator_address]['score'] += score
            else:
                creator_data[creator_address] = {
                    "score": score,
                    "data": creator,
                    "avatar": i['preview_url']
                }
                if not creator.get('user'):
                    creator['user'] = {}
                if not creator['user'].get('username'):
                    creator['user']['username'] = creator['address']

        except Exception as e:
            log.exception(e)
            pass

    return sorted(creator_data.values(), key=lambda x: x['score'], reverse=True)[:20]


def get_creators_for_art_ids(scores):
    art_keys = [{'art_id': i} for i in scores]
    creators = []
    for i in [art_keys[x:x + 100] for x in range(0, len(art_keys), 100)]:
        art_row = dynamodb.batch_get_item(
            RequestItems={
                'art': {
                    'Keys': i,
                    'ProjectionExpression': 'art_id, open_sea_data, preview_url'
                }
            }
        )
        creators.extend(art_row['Responses']['art'])

    return creators


def add_votes(scores, last_day):
    votes = dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("type").eq('vote') & Key("timestamp").gt(last_day),
        ScanIndexForward=False,
        IndexName='type-timestamp-index',
        ProjectionExpression="art_id, vote"
    )['Items']
    count_by_art_id(scores, votes, lambda i: int_or_zero(i['vote']))


def add_views(scores, last_day):
    views = dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("type").eq('view') & Key("timestamp").gt(last_day),
        ScanIndexForward=False,
        IndexName='type-timestamp-index',
        ProjectionExpression="art_id, vote"
    )['Items']
    count_by_art_id(scores, views, lambda i: 1)


def add_buys(scores, last_day):
    buys = dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("type").eq('buy') & Key("timestamp").gt(last_day),
        ScanIndexForward=False,
        IndexName='type-timestamp-index',
        ProjectionExpression="art_id, vote"
    )['Items']
    count_by_art_id(scores, buys, lambda i: 1)


def count_by_art_id(scores, d, fn_count):
    for art_id, vote in [[i['art_id'], fn_count(i)] for i in d]:
        scores[art_id] = scores.get(art_id, 0) + vote


def int_or_zero(v: str):
    try:
        return int(v) if v else 0
    except ValueError:
        return 0
