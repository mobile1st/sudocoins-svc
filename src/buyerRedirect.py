import hashlib
import hmac
import hashlib
import base64


class BuyerRedirect:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def getRedirect(self, userId, buyerName, survey, ip, transactionId, profile):

        if buyerName in ["test", "cint"]:
            url = f"{survey['url']}?si={survey['appId']}&ssi={transactionId}&unique_user_id={userId}&ip={ip}"
            message = 'unique_user_id=' + userId + '&ip=' + ip
            signature = hmac.new(
                survey['secretkey'].encode('utf-8'),
                msg=message.encode('utf-8'),
                digestmod=hashlib.md5
            ).hexdigest()

            entryUrl = url + '&hmac=' + signature

            return entryUrl

        elif buyerName in ["peanutLabs"]:
            checkSum = hashlib.md5((userId + survey['appId'] + survey['secretkey']).encode('utf-8'))
            peanutId = userId + "-" + survey['appId'] + "-" + checkSum.hexdigest()[:10]
            entryUrl = f"{survey['url']}?userId={peanutId}&sub_id={transactionId}"

            return entryUrl

        elif buyerName in ["dynata"]:
            checkSum = hashlib.md5((userId + survey['appId'] + survey['secretkey']).encode('utf-8'))
            peanutId = userId + "-" + survey['appId'] + "-" + checkSum.hexdigest()[:10]
            entryUrl = f"{survey['url']}?pub_id={survey['appId']}&sub_id={transactionId}&user_id={peanutId}"

            return entryUrl

        elif buyerName in ["lucid"]:
            try:
                countryCode = profile['lucidLocale']
            except Exception as e:
                print(e)
                countryCode = 0

            url = survey['url'] + "&sid=" + str(survey['appId']) + '&pid=' + userId \
                  + '&clid=' + str(countryCode) + '&mid=' + transactionId + '&'

            encodedKey = survey['secretkey'].encode('utf-8')
            encodedUrl = url.encode('utf-8')
            hashed = hmac.new(encodedKey, msg=encodedUrl, digestmod=hashlib.sha1)
            digestedHash = hashed.digest()
            base64_encoded_result = base64.b64encode(digestedHash)
            finalResult = base64_encoded_result.decode('utf-8').replace('+', '-').replace('/', '_').replace('=', '')

            entryUrl = url + 'hash=' + finalResult

            return entryUrl







