from decimal import Decimal


class RevenueData:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def get_revShare(self, data, buyerName):
        configTable = self.dynamodb.Table("Config")

        if buyerName in ['test', 'cint']:

            if not data["hashState"]:
                surveyCode = "F"
            else:
                surveyCode = data["queryStringParameters"]["c"]

            print("trying getConfig")
            buyerObject = configTable.get_item(Key={'configKey': 'TakeSurveyPage'})
            revenue = Decimal(buyerObject["Item"]["configValue"]["buyer"][buyerName]["defaultCpi"])
            surveyStatus = buyerObject["Item"]["configValue"]["buyer"][buyerName]["surveyStatus"]

            if data["queryStringParameters"]["c"] in surveyStatus:
                userStatus = surveyStatus[surveyCode]["userStatus"]
                revShare = Decimal(surveyStatus[surveyCode]["revShare"])
            else:
                userStatus = data["queryStringParameters"]["c"]
                revShare = Decimal(0)

            payment = revenue * revShare

            return revenue, payment, userStatus

        elif buyerName in ['peanutLabs']:
            surveyCode = data["queryStringParameters"]["c"]
            buyerObject = configTable.get_item(Key={'configKey': 'TakeSurveyPage'})
            revenue = Decimal(buyerObject["Item"]["configValue"]["buyer"][buyerName]["defaultCpi"])
            surveyStatus = buyerObject["Item"]["configValue"]["buyer"][buyerName]["surveyStatus"]

            if data["queryStringParameters"]["c"] in surveyStatus:
                userStatus = surveyStatus[surveyCode]["userStatus"]
                revShare = Decimal(surveyStatus[surveyCode]["revShare"])
            else:
                userStatus = data["queryStringParameters"]["c"]
                revShare = Decimal(0)

            payment = revenue * revShare

            return revenue, payment, userStatus, revShare