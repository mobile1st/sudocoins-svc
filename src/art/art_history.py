from decimal import Decimal
from boto3.dynamodb.conditions import Key
from datetime import datetime

import sudocoins_logger

log = sudocoins_logger.get()


class ArtHistory:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def updateProfile(self, userId):
        profileTable = self.dynamodb.Table("Profile")

        history = self.getHistory(userId)
        log.info(f"grabbed history {history}")

        topHistory = history[0:10]
        log.info(f"top 10 history {topHistory}")

        userProfile = profileTable.update_item(
            Key={
                "userId": userId
            },
            UpdateExpression="set history=:h",
            ExpressionAttributeValues={
                ":h": topHistory

            },
            ReturnValues="ALL_NEW"
        )

        return userProfile['Attributes']

    def getBalance(self, history):
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
            balance = 0

        return balance


    def getHistory(self, userId):
        ledger = self.getLedger(userId)
        log.info(f"ledger {ledger}")

        return history


    def getLedger(self, userId):
        ledgerTable = self.dynamodb.Table('Ledger')

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

        return ledger
