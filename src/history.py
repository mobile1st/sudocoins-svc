from decimal import Decimal
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime


class History:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb


    def getHistory(self, userId, rate, precision):
        ledger = loadLedger(userId, rate, precision)
        transactions = loadTransactions(userId)
        history = mergeHistory(ledger, transactions)

        return history


def loadTransactions(userId):
    """Fetches the user history from the Ledger table.
    Arguments: userId.
    Returns: a list of of objects, each representing a user's transaction.
    """
    dynamodb = boto3.resource('dynamodb')
    transactionTable = dynamodb.Table('Transaction')

    transactionHistory = transactionTable.query(
        KeyConditionExpression=Key("userId").eq(userId),
        ScanIndexForward=False,
        IndexName='userId-started-index',
        FilterExpression=Attr("payout").eq(0),
        ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
        ProjectionExpression="transactionId, started, #t, #s")

    transactions = transactionHistory["Items"]

    for i in transactions:
        if 'started' in i:
            utcTime = datetime.strptime(i['started'], "%Y-%m-%dT%H:%M:%S.%f")
            epochTime = int((utcTime - datetime(1970, 1, 1)).total_seconds())
            i['epochTime'] = epochTime

    return transactions


def loadLedger(userId, rate, precision):
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table("Ledger")

    try:
        ledgerHistory = ledgerTable.query(
            KeyConditionExpression=Key("userId").eq(userId),
            ScanIndexForward=False,
            IndexName='sortedHistory',
            ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
            ProjectionExpression="transactionId, lastUpdate, #t, #s, amount, payoutType, usdBtcRate, userInput")
        ledger = ledgerHistory["Items"]

        for i in ledger:
            if 'amount' in i:
                if i['amount'] == "":
                    i['amount'] = Decimal(0)
                else:
                    i['amount'] = str(((Decimal(i['amount'])) * rate).quantize(
                        Decimal('10') ** ((-1) * int(precision))))
            if 'lastUpdate' in i:
                utcTime = datetime.strptime(i['lastUpdate'], "%Y-%m-%dT%H:%M:%S.%f")
                epochTime = int((utcTime - datetime(1970, 1, 1)).total_seconds())
                i['epochTime'] = epochTime

            if 'payoutType' in i:
                if i['payoutType'] == 'Bitcoin':
                    bitcoin = str(
                        (Decimal(i['usdBtcRate']) * Decimal(i['userInput'])).quantize(Decimal('10') ** ((-1) * int(8))))
                    i['btcAmount'] = bitcoin

    except ClientError as e:
        print("Failed to query ledger for userId=%s error=%s", self, e.response['Error']['Message'])

        return {}

    else:
        return ledger


def mergeHistory(ledger, transactions):
    history = ledger + transactions
    print(history)
    history = sorted(history, key=lambda k: k['epochTime'], reverse=True)

    return history





