import hashlib


class BuyerRedirect:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def getRedirect(self, userId, buyerName, survey, ip, transactionId):

        if buyerName in ["test", "cint"]:
            entryUrl = f"{survey['url']}?si={survey['appId']}&ssi={transactionId}&unique_user_id={userId}&ip={ip}"

            return entryUrl

        elif buyerName in ['peanutLabs']:

            checkSum = hashlib.md5((userId + survey['appId'] + survey['secretkey']).encode('utf-8'))
            peanutId = userId + "-" + survey['appId'] + "-" + checkSum.hexdigest()[:10]

            entryUrl = f"{survey['url']}?pub_id={survey['appId']}&sub_id={transactionId}&user_id={peanutId}"

            return entryUrl
