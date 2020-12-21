import hashlib
import hmac
import hashlib
import base64
from history import History
from datetime import datetime
from decimal import Decimal
from rev_shares import RevenueData


class Transaction:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def endPL(self, data):
        history = History(self.dynamodb)
        surveyCode = data["queryStringParameters"]['status']
        transactionId = data["queryStringParameters"]['transactionId']
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

        return None

    def endDynata(self, data):
        history = History(self.dynamodb)
        transactionId = data["queryStringParameters"]['sub_id']
        surveyCode = data["queryStringParameters"]['status']
        updated = str(datetime.utcnow().isoformat())
        buyerName = data['buyerName']
        userId = data["queryStringParameters"]['endUserId']

        revData = RevenueData(self.dynamodb)
        revenue, payment, userStatus, revShare, cut = revData.get_revShare(data, buyerName)
        print("revShare data from class loaded")

        history.updateTransaction(transactionId, payment, surveyCode, updated,
                                  revenue, revShare, userStatus, cut, data, userId)
        print("Transaction updated")

        if payment > 0:
            history.createLedgerRecord(transactionId, payment, userId, updated, userStatus)
            print("Ledger updated")

        return None


    def endTest(self, data):
        history = History(self.dynamodb)
        transactionId = data["queryStringParameters"]['t']
        surveyCode = data["queryStringParameters"]['c']
        buyerName = data['buyerName']
        updated = str(datetime.utcnow().isoformat())

        revData = RevenueData(self.dynamodb)
        revenue, payment, userStatus, revShare, cut = revData.get_revShare(data, buyerName)
        print("revShare data from class loaded")

        history.updateTransaction(transactionId, payment, surveyCode, updated,
                                  revenue, revShare, userStatus, cut, data, userId)
        print("Transaction updated")

        if payment > 0:
            history.createLedgerRecord(transactionId, payment, userId, updated, userStatus)
            print("Ledger updated")

        return None


    def endLucid(self, data):
        history = History(self.dynamodb)

        transactionId = data["queryStringParameters"]['t']
        surveyCode = data["status"]

        updated = str(datetime.utcnow().isoformat())

        return

