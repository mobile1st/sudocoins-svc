import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    day, day3, day7 = get_new()

    set_config(day, day3, day7)

    return {
        "day": day,
        "day3": day3,
        "day7": day7
    }


def get_new():
    time_now = datetime.utcnow().isoformat()
    log.info(time_now)
    period = (datetime.fromisoformat(time_now) - timedelta(days=7)).isoformat()
    print(period)

    record = dynamodb.Table('collections').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("collection_date").gt(period),
        IndexName='collection_date-index',
        ProjectionExpression="collection_id, preview_url, collection_name, collection_date, blockchain, collection_url, sales_volume, open_sea_stats, collection_data, maximum, trades_delta",


    )
    data = record['Items']
    while 'LastEvaluatedKey' in record:
        record = dynamodb.Table('collections').query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("collection_date").gt(period),
            IndexName='collection_date-index',
            ProjectionExpression="collection_id, preview_url, collection_name, collection_date, blockchain, collection_url, sales_volume, open_sea_stats, collection_data, maximum, trades_delta",
            ExclusiveStartKey=record['LastEvaluatedKey']
        )
        data.extend(record['Items'])

    sorted_arts = sorted(data, key=lambda item: item['sales_volume'], reverse=True)

    day = []
    day3 = []
    day7 = []

    for i in sorted_arts:
        try:
            if i['collection_date'] > (datetime.fromisoformat(time_now) - timedelta(days=1)).isoformat():
                day.append(i)

            if i['collection_date'] > (datetime.fromisoformat(time_now) - timedelta(days=3)).isoformat():
                day3.append(i)

            if i['collection_date'] > (datetime.fromisoformat(time_now) - timedelta(days=7)).isoformat():
                day7.append(i)
        except Exception as e:
            log.info(e)
            log.info(i)

    return day, day3, day7


def set_config(day, day3, day7):
    config_table = dynamodb.Table('Config')

    response = config_table.update_item(
        Key={
            'configKey': 'NewCollections'
        },
        UpdateExpression="set day1=:day, day3=:day3, day7=:day7",
        ExpressionAttributeValues={
            ":day": day[0:100],
            ":day3": day3[0:100],
            ":day7": day7[0:100]
        },
        ReturnValues="ALL_NEW"
    )

    #. log.info(f"response: {response}")
    log.info("configs updated")


