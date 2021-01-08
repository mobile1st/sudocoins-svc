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
        print("PL Transaction updated")

        if payment > 0:
            history.createLedgerRecord(transactionId, payment, userId, completed, userStatus)
            print("PL Ledger updated")

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

        return None



