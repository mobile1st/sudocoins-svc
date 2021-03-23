from decimal import Decimal
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
import uuid
import sudocoins_logger

log = sudocoins_logger.get(__name__)


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

        log.debug("about to get orders")
        orders = self.getOrders(userId)
        log.info(f"orders {orders}")

        log.debug("about to merge")
        history = self.mergeHistory(ledger, transactions, orders)
        log.info(f"history {history}")
        return history

    def mergeHistory(self, ledger, transactions, orders):
        history = ledger + transactions + orders

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
                if 'amount' in i:
                    if i['amount'] == "":
                        i['amount'] = Decimal(0)
                    else:
                        i['amount'] = str(((Decimal(i['amount'])) * rate).quantize(
                            Decimal('10') ** ((-1) * int(precision))))
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
            ReturnValues="ALL_NEW"
        )
        log.info(f"updatedRecord: {updatedRecord}")

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
            'buyer': buyerName,
            "payout": 0
        }

        transactionTable.put_item(
            Item=transactionData
        )

        log.info("transaction record created")
        log.debug("about to update profile")
        userProfile = self.updateProfile(userId)

        return transactionData, userProfile

    def createOrder(self, orderRecord):
        ordersTable = self.dynamodb.Table('orders')
        ordersTable.put_item(
            Item=orderRecord
        )

    def updateOrder(self, orderId, orderStatus):
        ordersTable = self.dynamodb.Table('orders')
        response = ordersTable.update_item(
            Key={
                "orderId": orderId
            },
            UpdateExpression="set statusCode=:sc",
            ExpressionAttributeValues={
                ":sc": orderStatus
            },
            ReturnValues="ALL_NEW"
        )
        log.info(f"updated item: {response}")

        userId = response['Attributes']['userId']

        if orderStatus == "charge:confirmed":
            self.updateProfile(userId)
            client = boto3.client("sns")
            client.publish(
                PhoneNumber="+16282265769",
                Message="Gift Card ordered. Start processing"
            )

        else:
            self.updateProfile(userId)

    def getOrders(self, userId):
        ordersTable = self.dynamodb.Table('orders')

        orderHistory = ordersTable.query(
            KeyConditionExpression=Key("userId").eq(userId),
            ScanIndexForward=False,
            IndexName='userId-index',
            ProjectionExpression="orderId, created, amountUsd, statusCode")
        log.info(f"orderHistory {orderHistory}")
        orders = orderHistory["Items"]

        for i in orders:
            if 'created' in i:
                utcTime = datetime.strptime(i['created'], "%Y-%m-%dT%H:%M:%S.%f")
                epochTime = int((utcTime - datetime(1970, 1, 1)).total_seconds())
                i['epochTime'] = int(epochTime)
            i["status"] = i['statusCode'][7:]
            i['amount'] = str(Decimal(i['amountUsd']).quantize(
                Decimal(10) ** ((-1) * 2)))
            i['transactionId'] = i['orderId']
            i['type'] = 'Gift Card Order'

        return orders
