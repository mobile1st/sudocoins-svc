from boto3.dynamodb.conditions import Key
from datetime import datetime
import uuid
from util import sudocoins_logger

log = sudocoins_logger.get()


class Art:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def addLedgerRecord(self, amount, userId, type_value):
        ledgerTable = self.dynamodb.Table('Ledger')
        transactionId = str(uuid.uuid1())
        updated = str(datetime.utcnow().isoformat())

        ledgerTable.put_item(
            Item={
                'userId': userId,
                'transactionId': transactionId,
                'amount': amount,
                'status': 'Complete',
                'lastUpdate': updated,
                'type': type_value
            }
        )
        self.updateProfile(userId)

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

    def getHistory(self, userId):
        ledger = self.getLedger(userId)
        log.info(f"ledger {ledger}")

        return ledger


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

        ledger = sorted(ledger, key=lambda k: k['epochTime'], reverse=True)

        return ledger
