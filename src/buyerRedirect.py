import hashlib
import hmac
import hashlib
import base64


class BuyerRedirect:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def getRedirect(self, userId, buyerName, survey, ip, transactionId, cc):

        if buyerName in ["test", "cint"]:
            entryUrl = f"{survey['url']}?si={survey['appId']}&ssi={transactionId}&unique_user_id={userId}&ip={ip}"

            return entryUrl

        elif buyerName in ["peanutLabs"]:
            checkSum = hashlib.md5((userId + survey['appId'] + survey['secretkey']).encode('utf-8'))
            peanutId = userId + "-" + survey['appId'] + "-" + checkSum.hexdigest()[:10]
            entryUrl = f"{survey['url']}?userId={peanutId}"

            return entryUrl

        elif buyerName in ["dynata"]:
            checkSum = hashlib.md5((userId + survey['appId'] + survey['secretkey']).encode('utf-8'))
            peanutId = userId + "-" + survey['appId'] + "-" + checkSum.hexdigest()[:10]
            entryUrl = f"{survey['url']}?pub_id={survey['appId']}&sub_id={transactionId}&user_id={peanutId}"

            return entryUrl

        elif buyerName in ["lucid"]:
            try:
                countryCode = survey['countryCode'][cc[0]][cc[1]]
            except Exception as e:
                print(e)
                countryCode = 0

            url = survey['url'] + "&sid=" + survey['appId'] + '&pid=' + userId \
                  + '&clid=' + countryCode + '&mid=' + transactionId + '&'
            testUrl = url + 'tar=' + '1584274' + '&'

            encodedKey = survey['secretkey'].encode('utf-8')
            encodedUrl = testUrl.encode('utf-8')
            hashed = hmac.new(encodedKey, msg=encodedUrl, digestmod=hashlib.sha1)
            digestedHash = hashed.digest()
            base64_encoded_result = base64.b64encode(digestedHash)
            finalResult = base64_encoded_result.decode('utf-8').replace('+', '-').replace('/', '_').replace('=', '')

            entryUrl = testUrl + 'hash=' + finalResult

            return entryUrl







