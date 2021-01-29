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

        tmp = {"x": date, "y": i['Starts']}
        starts.append(tmp)

        tmp2 = {"x": date, "y": i['Completes']}
        completes.append(tmp2)

        tmp3 = {"x": date, "y": i['Revenue']}
        revenue.append(tmp3)

        tmp4 = {"x": date, "y": i['Profiles']}
        profiles.append(tmp4)

    starts.sort(key=lambda e: e['x'])
    completes.sort(key=lambda e: e['x'])
    profiles.sort(key=lambda e: e['x'])
    revenue.sort(key=lambda e: e['x'])

    return {
        "starts": starts,
        "completes": completes,
        "profiles": profiles,
        "revenue": revenue
    }
