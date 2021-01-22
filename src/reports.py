import boto3
from operator import itemgetter


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    reportsTable = dynamodb.Table('Reports')

    reports = reportsTable.scan()
    reports = reports['Items']

    starts = []
    completes = []
    profiles = []
    revenue = []

    for i in reports:
        date = i['date']
        starts.append([date, i['Starts']])
        completes.append([date, i['Completes']])
        revenue.append([date, i['Revenue']])
        profiles.append([date, i['Profiles']])

    starts.sort(key=itemgetter(0), reverse=False)
    completes.sort(key=itemgetter(0), reverse=False)
    revenue.sort(key=itemgetter(0), reverse=False)
    profiles.sort(key=itemgetter(0), reverse=False)

    return {
        "starts": starts,
        "completes": completes,
        "profiles": profiles,
        "revenue": revenue
    }
