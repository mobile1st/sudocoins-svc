from history import History
import boto3
from datetime import datetime
from rev_shares import RevenueData
import json


class Transaction:

    def __init__(self, dynamodb, sns_client):
        self.dynamodb = dynamodb
        self.sns_client = sns_client

    def endPL(self, data):
        history = History(self.dynamodb)
        surveyCode = data["queryStringParameters"]['status']
        print(surveyCode)
        transactionId = data["queryStringParameters"]['transactionId']
        print(transactionId)
        completed = str(datetime.utcnow().isoformat())
        buyerName = data['buyerName']
        userId = data["queryStringParameters"]['endUserId']

        revData = RevenueData(self.dynamodb)
        revenue, payment, userStatus, revShare, cut = revData.get_revShare(data, buyerName)
        print("revShare data from class loaded")

        history.updateTransaction(transactionId, payment, surveyCode, completed,
                                  revenue, revShare, userStatus, cut, data, userId)
        print("Transaction updated")

        if payment > 0:
            history.createLedgerRecord(transactionId, payment, userId, completed, userStatus)
            print("Ledger updated")

        # TODO convert surveyCode to buyer independent format (C, ...)
        self.send_notification(surveyCode, transactionId, completed, 'peanutlabs', userId, revenue)


        return None

    def endDynata(self, data):
        print("start dynata")
        history = History(self.dynamodb)
        transactionId = data["queryStringParameters"]['sub_id']
        print(transactionId)
        surveyCode = data["queryStringParameters"]['status']
        print(surveyCode)
        updated = str(datetime.utcnow().isoformat())
        buyerName = data['buyerName']
        userId = data["queryStringParameters"]['endUserId']

        revData = RevenueData(self.dynamodb)
        revenue, payment, userStatus, revShare, cut = revData.get_revShare(data, buyerName)
        print("revShare data from class loaded")
        print(revenue)
        print(userStatus)

        history.updateTransaction(transactionId, payment, surveyCode, updated,
                                  revenue, revShare, userStatus, cut, data, userId)
        print("Transaction updated")

        if payment > 0:
            history.createLedgerRecord(transactionId, payment, userId, updated, userStatus)
            print("Ledger updated")

        # TODO convert surveyCode to buyer independent format (C, ...)
        self.send_notification(surveyCode, transactionId, updated, 'dynata', userId, revenue)

        return None


    def endTest(self, data):
        #  also used for Cint
        history = History(self.dynamodb)
        transactionId = data["queryStringParameters"]['sid']
        surveyCode = data["queryStringParameters"]['status']
        buyerName = data['buyerName']
        updated = str(datetime.utcnow().isoformat())

        dynamodb = boto3.resource('dynamodb')
        transactionTable = dynamodb.Table('Transaction')
        transactionResponse = transactionTable.get_item(Key={'transactionId': transactionId})
        userId = transactionResponse['Item']['userId']

        revData = RevenueData(self.dynamodb)
        revenue, payment, userStatus, revShare, cut = revData.get_revShare(data, buyerName)
        print("revShare data from class loaded")

        history.updateTransaction(transactionId, payment, surveyCode, updated,
                                  revenue, revShare, userStatus, cut, data, userId)
        print("Transaction updated")

        if payment > 0:
            history.createLedgerRecord(transactionId, payment, userId, updated, userStatus)
            print("Ledger updated")

        # TODO convert surveyCode to buyer independent format (C, ...)
        self.send_notification(surveyCode, transactionId, updated, 'cint', userId, revenue)

        return None


    def endLucid(self, data):
        transactionId = data['transactionId']
        surveyCode = data["status"]
        buyerName = data['buyerName']
        updated = str(datetime.utcnow().isoformat())
        userId = data["userId"]

        revData = RevenueData(self.dynamodb)
        revenue, payment, userStatus, revShare, cut = revData.get_revShare(data, buyerName)

        history = History(self.dynamodb)
        history.updateTransaction(transactionId, payment, surveyCode, updated,
                                  revenue, revShare, userStatus, cut, data, userId)
        print("Transaction updated")

        if payment > 0:
            history.createLedgerRecord(transactionId, payment, userId, updated, userStatus)
            print("Ledger updated")

        # TODO convert surveyCode to buyer independent format (C, ...)
        self.send_notification(surveyCode, transactionId, updated, 'lucid', userId, revenue)

        return None


    def send_notification(self, status, transaction_id, timestamp, buyer_name, user_id, revenue):
        self.sns_client.publish(
            TopicArn="arn:aws:sns:us-west-2:977566059069:transaction-event",
            MessageStructure='string',
            Message=json.dumps({
                "source": 'SURVEY_END',
                "status": status,
                "transactionId": transaction_id,
                "timestamp": timestamp,
                "buyerName": buyer_name,
                "userId": user_id,
                "revenue": revenue
            })
        )


