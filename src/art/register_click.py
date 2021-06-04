import boto3
import json
from util import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.debug('art_counter called')
    for record in event['Records']:
        payload = record['body']
        log.info(f'payload: {payload}')

        data = json.loads(payload)
        register_click(data)

        log.info('record updated')


def register_click(data):
    # if a user's share url
    if 'shareId' in data:
        print("shareId:" + data['shareId'])
        # update view count for art in art_uploads
        dynamodb.Table('art_uploads').update_item(
            Key={'shareId': data['shareId']},
            UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
            ExpressionAttributeValues={
                ':inc': 1,
                ':start': 0
            },
            ReturnValues="UPDATED_NEW"
        )
        print("art_uploads click_count increased")
        # get some data about this art in art_uploads
        art_uploads_record = dynamodb.Table('art_uploads').get_item(
            Key={'shareId': data['shareId']},
            ProjectionExpression="art_id, user_id")['Item']
        log.debug(f'art_uploads_record: {art_uploads_record}')
        if 'user_id' in art_uploads_record:
            update_user_count(art_uploads_record['user_id'])

        # get art record in art table and update click count
        if 'art_id' in art_uploads_record:
            art_id = art_uploads_record['art_id']
            dynamodb.Table('art').update_item(
                Key={'art_id': art_id},
                UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':start': 0
                },
                ReturnValues="UPDATED_NEW"
            )
            print("art record click count increased")

    # if it's not a custom art url, then it's a generic art url
    elif 'art_id' in data:
        # add to click count
        dynamodb.Table('art').update_item(
            Key={'art_id': data['art_id']},
            UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
            ExpressionAttributeValues={
                ':inc': 1,
                ':start': 0
            },
            ReturnValues="UPDATED_NEW"
        )
        print("art record click count increased")
        # no need to add points to user profile


def update_user_count(user_id):
    # update user profile click_count
    dynamodb.Table('Profile').update_item(
        Key={'userId': user_id},
        UpdateExpression="SET click_count = if_not_exists(click_count, :start) + :inc",
        ExpressionAttributeValues={
            ':inc': 1,
            ':start': 0
        },
        ReturnValues="UPDATED_NEW"
    )
    print("user profile click_count increased")
    # get some user data about click count
    profile_record = dynamodb.Table('Profile').get_item(
        Key={'userId': user_id},
        ProjectionExpression="click_count, click_count_paid")
    click_count = profile_record['Item']['click_count']
    if 'click_count_paid' in profile_record['Item']:
        click_count_paid = profile_record['Item']['click_count_paid']
    else:
        click_count_paid = 0
    # pay user if they earned sudocoins
    if click_count - click_count_paid > 100:
        dynamodb.Table('Profile').update_item(
            Key={'userId': user_id},
            UpdateExpression="SET click_count_paid = if_not_exists(click_count_paid, :start) + :inc, "
                             "sudocoins = if_not_exists(sudocoins, :start) + :inc2",
            ExpressionAttributeValues={
                ':inc': 100,
                ':start': 0,
                ':inc2': 10
            },
            ReturnValues="UPDATED_NEW"
        )
        art.addLedgerRecord(5, user_id, '100 Views')
