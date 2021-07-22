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
    log.info(scores)

    influencers = get_influencers(scores)
    log.info(influencers)

    set_config(influencers)
    log.info("influencers leaderboard config set")

    return influencers


def get_influencers(scores):
    dynamodb = boto3.resource('dynamodb')
    profiles = []

    query = {
        'Keys': [{'userId': i[0]} for i in scores],
        'ProjectionExpression': 'userId, email, user_name, twitter_handle, gravatarEmail'
    }
    response = dynamodb.batch_get_item(RequestItems={'Profile': query})

    print(response)

    influencers = response['Responses']['Profile']
    profiles.append(influencers)

    scores = dict(scores)
    print(scores)
    for i in influencers:
        click_count = scores[i['userId']]['score']
        i['click_count'] = click_count

    arts = []
    arts_dict = {}
    for i in scores:
        print(scores[i]['art_id'])
        if scores[i]['art_id'] not in arts_dict:
            print("here")
            element = {'art_id': scores[i]['art_id']}
            arts.append(element)
            arts_dict[scores[i]['art_id']] = 0
    print(arts)
    art_rows = dynamodb.batch_get_item(
        RequestItems={
            'art': {
                'Keys': arts,
                'ProjectionExpression': 'art_id, preview_url'
            }
        }
    )
    avatars = art_rows['Responses']['art']
    print(avatars)

    art_keys = {}
    for i in avatars:
        art_keys[i['art_id']] = i['preview_url']

    for i in scores:
        preview_url = art_keys[scores[i]['art_id']]
        # print(i)
        for k in influencers:
            print(k)
            if i == k['userId']:
                k['avatar'] = preview_url

    newlist = sorted(influencers, key=lambda k: k['click_count'], reverse=True)

    return newlist


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
        del arts['total']
        b = list(arts.keys())[0]
        scores[i]['art_id'] = b

    sorted_dict = OrderedDict(sorted(scores.items(), key=lambda x: getitem(x[1], 'score'), reverse=True))
    top_20 = list(sorted_dict.items())[:20]

    return top_20
