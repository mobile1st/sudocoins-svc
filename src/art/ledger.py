from boto3.dynamodb.conditions import Key
from datetime import datetime
import uuid
from util import sudocoins_logger

log = sudocoins_logger.get()


class Ledger:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb
        self.ledger_table = self.dynamodb.Table('Ledger')

    def add(self, amount, user_id, type_value):
        self.ledger_table.put_item(
            Item={
                'userId': user_id,
                'transactionId': str(uuid.uuid1()),
                'amount': amount,
                'status': 'Complete',
                'lastUpdate': str(datetime.utcnow().isoformat()),
                'type': type_value
            }
        )
        self.__update_profile(user_id)

    def __update_profile(self, user_id):
        history = self.__get_history(user_id)
        log.info(f"grabbed history {history}")

        top_history = history[0:10]
        log.info(f"top 10 history {top_history}")

        profile = self.dynamodb.Table("Profile").update_item(
            Key={
                "userId": user_id
            },
            UpdateExpression="set history=:h",
            ExpressionAttributeValues={
                ":h": top_history
            },
            ReturnValues="ALL_NEW"
        )

        return profile['Attributes']

    def __get_history(self, user_id):
        ledger_history = self.ledger_table.query(
            KeyConditionExpression=Key("userId").eq(user_id),
            ScanIndexForward=False,
            IndexName='sortedHistory',
            ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
            ProjectionExpression="transactionId, lastUpdate, #t, #s, amount, payout_type, usdBtcRate, userInput")
        ledger = ledger_history["Items"]
        for i in ledger:
            if 'lastUpdate' in i:
                utc_time = datetime.strptime(i['lastUpdate'], "%Y-%m-%dT%H:%M:%S.%f")
                epoch_time = int((utc_time - datetime(1970, 1, 1)).total_seconds())
                i['epochTime'] = int(epoch_time)

        ledger = sorted(ledger, key=lambda k: k['epochTime'], reverse=True)
        log.info(f"ledger {ledger}")
        return ledger
