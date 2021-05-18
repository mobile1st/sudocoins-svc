from util import sudocoins_logger
from decimal import Decimal

log = sudocoins_logger.get()


class RevenueData:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def get_revShare(self, data, buyerName):
        configTable = self.dynamodb.Table("Config")

        if buyerName in ['test', 'cint']:

            if not data["hashState"]:
                surveyCode = "F"
            else:
                surveyCode = data["queryStringParameters"]["status"]

            log.info("trying getConfig")
            buyerObject = configTable.get_item(Key={'configKey': 'TakeSurveyPage'})
            revenue = Decimal(buyerObject["Item"]["configValue"]["buyer"][buyerName]["defaultCpi"])
            surveyStatus = buyerObject["Item"]["configValue"]["buyer"][buyerName]["surveyStatus"]

            if data["queryStringParameters"]["status"] in surveyStatus:
                userStatus = surveyStatus[surveyCode]["userStatus"]
                revShare = Decimal(surveyStatus[surveyCode]["revShare"])
                if 'cut' in surveyStatus[surveyCode]:
                    cut = Decimal(surveyStatus[surveyCode]['cut'])
                else:
                    cut = Decimal(0)
            else:
                userStatus = data["queryStringParameters"]["status"]
                revShare = Decimal(0)
                cut = Decimal(0)

            payment = revenue * revShare

            return revenue, payment, userStatus, revShare, cut

        elif buyerName in ['peanutLabs', 'dynata']:
            surveyCode = data["queryStringParameters"]["status"]
            buyerObject = configTable.get_item(Key={'configKey': 'TakeSurveyPage'})

            if 'amt' in data["queryStringParameters"]:
                revenue = Decimal(data["queryStringParameters"]['amt'])*100
            else:
                revenue = Decimal('0.00')

            surveyStatus = buyerObject["Item"]["configValue"]["buyer"][buyerName]["surveyStatus"]

            if surveyCode in surveyStatus:
                userStatus = surveyStatus[surveyCode]["userStatus"]
                revShare = Decimal(surveyStatus[surveyCode]["revShare"])
                if 'cut' in surveyStatus[surveyCode]:
                    cut = Decimal(surveyStatus[surveyCode]['cut'])
                else:
                    cut = Decimal(0)
            else:
                userStatus = data["queryStringParameters"]["status"]
                revShare = Decimal(0)
                cut = Decimal(0)

            payment = revenue * revShare

            return revenue, payment, userStatus, revShare, cut

        elif buyerName in ['lucid']:
            surveyCode = data["status"]
            buyerObject = configTable.get_item(Key={'configKey': 'TakeSurveyPage'})
            surveyStatus = buyerObject["Item"]["configValue"]["buyer"][buyerName]["surveyStatus"]

            if 'revenue' in data:
                revenue = Decimal(data['revenue']) * 100
            else:
                revenue = Decimal('0.00')

            if surveyCode in surveyStatus:
                userStatus = surveyStatus[surveyCode]["userStatus"]
                revShare = Decimal(surveyStatus[surveyCode]["revShare"])
                log.info(f'revShare: {revShare}')
                if 'cut' in surveyStatus[surveyCode]:
                    cut = Decimal(surveyStatus[surveyCode]['cut'])
                else:
                    cut = Decimal(0)
                if 'lucidCut' in surveyStatus[surveyCode]:
                    lucidCut = Decimal(surveyStatus[surveyCode]['lucidCut'])
                else:
                    lucidCut = Decimal('1')

            else:
                userStatus = data["status"]
                revShare = Decimal(0)
                cut = Decimal(0)

            log.info(f'revenue {revenue}')
            log.info(f'lucidCut {lucidCut}')
            log.info(f'revShare {revShare}')
            payment = revenue * lucidCut * revShare
            revenue = revenue * lucidCut

            return revenue, payment, userStatus, revShare, cut

