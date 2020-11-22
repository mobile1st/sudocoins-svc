from decimal import Decimal
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import uuid


class History:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb


    def updateProfile(self, userId):
        profileTable = self.dynamodb.Table("Profile")
        history = self.getHistory(userId)
        balance = self.getBalance(self, history)
        topHistory = history[0:10]
        profileTable.update_item(
            Key={
                "userId": userId
            },
            UpdateExpression="set history=:h, balance=:b",
            ExpressionAttributeValues={
                ":h": topHistory,
                ":b": balance

            },
            ReturnValues="ALL_NEW"
        )


    def getBalance(self, history):
        precision = 2  # 2 decimals for usd
        debit = 0
        credit = 0

        for i in history:
            if 'type' in i.keys():
                if i["type"] == "Cash Out":
                    credit += Decimal(i["amount"])

                elif 'amount' in i.keys() and i['amount'] != "":
                    debit += Decimal(i["amount"])

        balance = debit - credit

        if balance <= 0:
            precision = 2
            balance = str(Decimal(0).quantize(Decimal(10) ** ((-1) * int(precision))))
        else:
            balance = str(balance.quantize(Decimal(10) ** ((-1) * int(precision))))

        return balance


    def getHistory(self, userId, rate, precision):
        ledger = self.getLedger(userId, rate, precision)
        transactions = self.getTransactions(userId)
        history = self.mergeHistory(ledger, transactions)

        return history


    def mergeHistory(self, ledger, transactions):
        history = ledger + transactions
        print(history)
        history = sorted(history, key=lambda k: k['epochTime'], reverse=True)

        return history


    def getTransactions(self, userId):
        transactionTable = self.dynamodb.Table('Transaction')

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



    def getLedger(self, userId, rate, precision):
        ledgerTable = self.dynamodb.Table('Transaction')

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
                            (Decimal(i['usdBtcRate']) * Decimal(i['userInput'])).quantize(
                                Decimal('10') ** ((-1) * int(8))))
                        i['btcAmount'] = bitcoin

        except ClientError as e:
            print("Failed to query ledger for userId=%s error=%s", self, e.response['Error']['Message'])

            return {}

        else:
            return ledger

    '''
    def updateLedger(self):
        # logic to update record
        # update profile
        return
    '''

    def createLedgerRecord(self, transactionId, payment, userId, updated, userStatus):
        ledgerTable = self.dynamodb.Table('Ledger')
        updatedRecord = ledgerTable.put_item(

            Item={
                'userId': userId,
                'transactionId': transactionId,
                'amount': payment,
                'status': userStatus,
                'lastUpdate': updated,
                'type': 'Survey'
            }
        )

        self.updateProfile(userId)

        return updatedRecord


    def updateTransaction(self, transactionId, payment, surveyCode,
                          updated, revenue, revShare, userStatus, cut, data, userId):
        transactionTable = self.dynamodb.Table('Transaction')
        updatedRecord = transactionTable.update_item(
            Key={
                'transactionId': transactionId
            },
            UpdateExpression="set payout=:pay, #status1=:s, completed=:c, redirect=:r, revenue=:rev, revShare=:rs, "
                             "surveyCode=:sc",
            ExpressionAttributeNames={
                "#status1": "status"
            },
            ExpressionAttributeValues={
                ":pay": payment,
                ":s": userStatus,
                ":c": updated,
                ":r": data,
                ":rev": revenue * cut,
                ":rs": revShare,
                ":sc": surveyCode
            },
            ReturnValues="UPDATED_NEW"
        )

        self.updateProfile(userId)

        return updatedRecord


    def insertTransactionRecord(self, userId, buyerName, ip):
        transactionTable = self.dynamodb.Table('Transaction')

        transactionId = uuid.uuid1()
        started = datetime.utcnow().isoformat()

        transactionData = {
            'transactionId': str(transactionId),
            "userId": userId,
            'status': "Started",
            'type': 'Survey',
            'ip': ip,
            'started': str(started),
            'buyer': buyerName
        }

        transactionTable.put_item(
            Item=transactionData
        )

        self.updateProfile(userId)

        return transactionData


