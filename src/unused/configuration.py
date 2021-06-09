

class Configuration:

    def __init__(self, dynamodb, cache=None):
        self.dynamodb = dynamodb
        self.cache = cache
        self.take_survey_config = None

    def buyer(self, name):
        buyers, _ = self._get_take_survey_key()
        return buyers[name]

    def public_buyers(self):
        _, public_buyers = self._get_take_survey_key()
        return public_buyers

    def _get_take_survey_key(self):
        print('Configuration read=take_survey')
        if self.take_survey_config is not None:
            print('Configuration found in memory')
            return self.take_survey_config

        buyers, public_buyers = self._load_take_survey_key_disk()
        if buyers is not None and public_buyers is not None:
            self.take_survey_config = (buyers, public_buyers)
            print('Configuration loaded form disk')
            return self.take_survey_config

        print('Configuration not on disk, loading from database')
        buyers, public_buyers = self._load_take_survey_key_db()
        self.take_survey_config = (buyers, public_buyers)
        print('Configuration loaded form database')
        return self.take_survey_config

    def _load_take_survey_key_disk(self):
        # todo using self.cache
        return None, None

    def _load_take_survey_key_db(self):
        config = self.dynamodb.Table("Config")
        row = config.get_item(Key={'configKey': 'TakeSurveyPage'})
        take_survey = row["Item"]["configValue"]
        buyers = {}
        for name, cfg in take_survey["buyer"].items():
            buyers[name] = BuyerConfiguration(cfg)

        return buyers, take_survey["publicBuyers"]


class BuyerConfiguration:
    def __init__(self, settings):
        self.surveyStatus = settings.get('surveyStatus')
        self.secretKey = settings.get('secretKey')
        self.revShare = settings.get('revShare')
        self.defaultCpi = settings.get('defaultCpi')
        self.buyerId = settings.get('buyerId')
        self.url = settings.get('url')
        self.createdAt = settings.get('createdAt')
        self.maxCpi = settings.get('maxCpi')
        self.appId = settings.get('appId')
        self.name = settings.get('name')
        self.iconLocation = settings.get('iconLocation')
        self.parameters = settings.get('parameters')
        self.updatedAt = settings.get('updatedAt')

