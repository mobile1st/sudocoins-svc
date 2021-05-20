from util import sudocoins_logger
from decimal import Decimal

log = sudocoins_logger.get()


class RevenueData:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def get_rev_share(self, data, buyer_name):
        config_table = self.dynamodb.Table("Config")

        if buyer_name in ['test', 'cint']:

            if not data["hashState"]:
                survey_code = "F"
            else:
                survey_code = data["queryStringParameters"]["status"]

            log.info("trying getConfig")
            buyer_object = config_table.get_item(Key={'configKey': 'TakeSurveyPage'})
            revenue = Decimal(buyer_object["Item"]["configValue"]["buyer"][buyer_name]["defaultCpi"])
            survey_status = buyer_object["Item"]["configValue"]["buyer"][buyer_name]["surveyStatus"]

            if data["queryStringParameters"]["status"] in survey_status:
                user_status = survey_status[survey_code]["userStatus"]
                rev_share = Decimal(survey_status[survey_code]["revShare"])
                if 'cut' in survey_status[survey_code]:
                    cut = Decimal(survey_status[survey_code]['cut'])
                else:
                    cut = Decimal(0)
            else:
                user_status = data["queryStringParameters"]["status"]
                rev_share = Decimal(0)
                cut = Decimal(0)

            payment = revenue * rev_share

            return revenue, payment, user_status, rev_share, cut

        elif buyer_name in ['peanutLabs', 'dynata']:
            survey_code = data["queryStringParameters"]["status"]
            buyer_object = config_table.get_item(Key={'configKey': 'TakeSurveyPage'})

            if 'amt' in data["queryStringParameters"]:
                revenue = Decimal(data["queryStringParameters"]['amt'])*100
            else:
                revenue = Decimal('0.00')

            survey_status = buyer_object["Item"]["configValue"]["buyer"][buyer_name]["surveyStatus"]

            if survey_code in survey_status:
                user_status = survey_status[survey_code]["userStatus"]
                rev_share = Decimal(survey_status[survey_code]["revShare"])
                if 'cut' in survey_status[survey_code]:
                    cut = Decimal(survey_status[survey_code]['cut'])
                else:
                    cut = Decimal(0)
            else:
                user_status = data["queryStringParameters"]["status"]
                rev_share = Decimal(0)
                cut = Decimal(0)

            payment = revenue * rev_share

            return revenue, payment, user_status, rev_share, cut

        elif buyer_name in ['lucid']:
            survey_code = data["status"]
            buyer_object = config_table.get_item(Key={'configKey': 'TakeSurveyPage'})
            survey_status = buyer_object["Item"]["configValue"]["buyer"][buyer_name]["surveyStatus"]

            if 'revenue' in data:
                revenue = Decimal(data['revenue']) * 100
            else:
                revenue = Decimal('0.00')

            if survey_code in survey_status:
                user_status = survey_status[survey_code]["userStatus"]
                rev_share = Decimal(survey_status[survey_code]["revShare"])
                log.info(f'revShare: {rev_share}')
                if 'cut' in survey_status[survey_code]:
                    cut = Decimal(survey_status[survey_code]['cut'])
                else:
                    cut = Decimal(0)
                if 'lucidCut' in survey_status[survey_code]:
                    lucid_cut = Decimal(survey_status[survey_code]['lucidCut'])
                else:
                    lucid_cut = Decimal('1')

            else:
                user_status = data["status"]
                rev_share = Decimal(0)
                cut = Decimal(0)

            log.info(f'revenue {revenue}')
            log.info(f'lucidCut {lucid_cut}')
            log.info(f'revShare {rev_share}')
            payment = revenue * lucid_cut * rev_share
            revenue = revenue * lucid_cut

            return revenue, payment, user_status, rev_share, cut

