

class Configuration:

    def __init__(self, dynamodb, cache):
        self.dynamodb = dynamodb
        self.cache = cache

    def get_buyer(self, name):
        # todo 1: look in filesystem 2: load from dynamodb
        buyer = self.load_buyer(name)
        return BuyerConfiguration(buyer)

    def load_buyer(self, name):
        configTable = self.dynamodb.Table("Config")
        buyerObject = configTable.get_item(Key={'configKey': 'TakeSurveyPage'})
        return buyerObject["Item"]["configValue"]["buyer"][name]


class BuyerConfiguration:
    def __init__(self, settings):
        self.surveyStatus = settings['surveyStatus']
        self.secretKey = settings['secretKey']
        self.revShare = settings['revShare']
        self.defaultCpi = settings['defaultCpi']
        self.buyerId = settings['buyerId']
        self.url = settings['url']
        self.createdAt = settings['createdAt']
        self.maxCpi = settings['maxCpi']
        self.appId = settings['appId']
        self.name = settings['name']
        self.iconLocation = settings['iconLocation']
        self.parameters = settings['parameters']
        self.updatedAt = settings['updatedAt']

