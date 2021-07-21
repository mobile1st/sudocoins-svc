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
    scores = get_views(last_day)
    log.info("calculated top influencers based on views")

    leaders = get_influencers(scores)
    log.info(leaders)

    #  set_config(leaders)
    #  log.info("influencers leaderboard config set")

    return leaders


def get_influencers(scores):
    client = boto3.client('dynamodb')
    profiles = []
    for i in scores:
        element = {'userId': i}
        profiles.append(element)

    profile_rows = client.batch_get_item(
        RequestItems={
            'profiles': {
                'Keys': i,
                'ProjectionExpression': 'userId, email, user_name, twitter_handle, gravatarEmail'
            }
        }
    )
    influencers = profile_rows['Responses']['profiles']
    profiles.append(influencers)

    for i in influencers:
        click_count = scores[i['userId']]['score']
        i['click_count'] = click_count

    arts = []
    for i in scores:
        element = {'art_id': i['art_id']}
        arts.append(element)

    art_rows = client.batch_get_item(
        RequestItems={
            'arts': {
                'Keys': i,
                'ProjectionExpression': 'art_id, preview_url'
            }
        }
    )
    avatars = art_rows['Responses']['arts']
    arts.append(avatars)

    art_keys = {}
    for i in arts:
        art_keys[i['art_id']] = i['preview_url']

    for i in scores:
        preview_url = art_keys[i['art_id']]
        for k in influencers:
            if i in k:
                k['avatar'] = preview_url

    return influencers


def set_config(leaders):
    config_table = dynamodb.Table('Config')

    updated_leaderboard = config_table.update_item(
        Key={
            'configKey': 'Leaderboard'
        },
        UpdateExpression="set leaders=:lead",
        ExpressionAttributeValues={
            ":lead": leaders
        },
        ReturnValues="ALL_NEW"
    )

    print(updated_leaderboard)


def get_views(last_day):
    views = dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("type").eq('view') & Key("timestamp").gt(last_day),
        ScanIndexForward=False,
        FilterExpression="attribute_exists(influencer)",
        IndexName='type-timestamp-index',
        ProjectionExpression="art_id, influencer"
    )['Items']
    # create a dictionary that maps art_id=>view_count int
    view_counts = {}
    for i in views:
        if i['influencer'] in view_counts:
            if i['art_id'] in view_counts[i['influencer']]:
                view_counts[i['influencer']][i['art_id']] += 1
                view_counts[i['influencer']]['total'] += 1
            else:
                view_counts[i['influencer']][i['art_id']] = 1
                view_counts[i['influencer']]['total'] += 1
        else:
            view_counts[i['influencer']] = {i['art_id']: 1}
            view_counts[i['influencer']]['total'] = 1

    scores = {}
    for i in view_counts:
        scores[i] = {'score': view_counts[i]['total']}
        arts = view_counts[i]
        arts = {k: v for k, v in sorted(arts.items(), key=lambda item: item[1], reverse=True)}
        b = list(arts.keys())[1]
        scores[i]['art_id'] = b

    sorted_dict = OrderedDict(sorted(scores.items(), key=lambda x: getitem(x[1], 'score'), reverse=True))
    top_20 = list(sorted_dict.items())[:20]

    return top_20
