from decimal import Decimal
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import uuid
from util import sudocoins_logger

log = sudocoins_logger.get()


class History:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def updateProfile(self, userId):
        profileTable = self.dynamodb.Table("Profile")
        history = self.getHistory(userId)
        log.info(f"grabbed history {history}")

        log.debug("getting balance")
        balance = self.getBalance(history)
        log.info(f"got balance {balance}")

        topHistory = history[0:10]
        log.info(f"top 10 history {topHistory}")

        userProfile = profileTable.update_item(
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

        return userProfile['Attributes']

    def getBalance(self, history):
        precision = 2  # 2 decimals for usd
        debit = 0
        credit = 0

        for i in history:
            if 'type' in i.keys():
                if i["type"] == "Cash Out":
                    credit += Decimal(i["amount"])

                elif i['type'] == "Gift Card Order":
                    pass

                elif 'amount' in i.keys() and i['amount'] != "":
                    debit += Decimal(i["amount"])

        balance = debit - credit

        if balance <= 0:
            precision = 2
            balance = str(Decimal(0).quantize(Decimal(10) ** ((-1) * int(precision))))
        else:
            balance = str(balance.quantize(Decimal(10) ** ((-1) * int(precision))))

        return balance

    def getHistory(self, userId):
        rate = Decimal('.01')
        precision = 2

        log.debug("about to get ledger")
        ledger = self.getLedger(userId, rate, precision)
        log.info(f"ledger {ledger}")

        log.debug("about to get transaction")
        transactions = self.getTransactions(userId)
        log.info(f"transactions {transactions}")

        log.debug("about to merge")
        history = self.mergeHistory(ledger, transactions)
        log.info(f"history {history}")
        return history

    def mergeHistory(self, ledger, transactions):
        history = ledger + transactions

        history = sorted(history, key=lambda k: k['epochTime'], reverse=True)
        log.info("history sorted")
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
                i['epochTime'] = int(epochTime)

        return transactions

    def getLedger(self, userId, rate, precision):
        ledgerTable = self.dynamodb.Table('Ledger')

        try:
            ledgerHistory = ledgerTable.query(
                KeyConditionExpression=Key("userId").eq(userId),
                ScanIndexForward=False,
                IndexName='sortedHistory',
                ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
                ProjectionExpression="transactionId, lastUpdate, #t, #s, amount, payoutType, usdBtcRate, userInput")
            ledger = ledgerHistory["Items"]

            for i in ledger:

                if 'lastUpdate' in i:
                    utcTime = datetime.strptime(i['lastUpdate'], "%Y-%m-%dT%H:%M:%S.%f")
                    epochTime = int((utcTime - datetime(1970, 1, 1)).total_seconds())
                    i['epochTime'] = int(epochTime)

                if 'payoutType' in i:
                    if i['payoutType'] == 'Bitcoin' or i['payoutType'] == 'Ethereum':
                        bitcoin = str(
                            (Decimal(i['usdBtcRate']) * Decimal(i['userInput'])).quantize(
                                Decimal('10') ** ((-1) * int(8))))
                        i['btcAmount'] = bitcoin

        except ClientError:
            log.exception("Failed to query ledger")

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
                             "surveyCode=:sc, userId=:ui",
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
                ":sc": surveyCode,
                ":ui": userId
            },
            ReturnValues="ALL_NEW"
        )
        log.info(f"updatedRecord: {updatedRecord}")

        self.updateProfile(userId)

        return updatedRecord

    def insertTransactionRecord(self, userId, buyerName, ip, fraud_score, ipqs):
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
            'buyer': buyerName,
            "payout": 0,
            "fraud_score": fraud_score,
            "ipqs": ipqs
        }

        transactionTable.put_item(
            Item=transactionData
        )

        log.info("transaction record created")
        log.debug("about to update profile")
        userProfile = self.updateProfile(userId)

        return transactionData, userProfile