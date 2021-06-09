import hmac
import hashlib
import base64


class BuyerRedirect:

    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    @staticmethod
    def get_redirect(user_id, buyer_name, survey, ip, transaction_id, profile):

        if buyer_name in ["test", "cint"]:
            url = f"{survey['url']}?si={survey['appId']}&ssi={transaction_id}&unique_user_id={user_id}&ip={ip}"
            message = 'unique_user_id=' + user_id + '&ip=' + ip
            signature = hmac.new(
                survey['secretkey'].encode('utf-8'),
                msg=message.encode('utf-8'),
                digestmod=hashlib.md5
            ).hexdigest()

            entry_url = url + '&hmac=' + signature

            return entry_url

        elif buyer_name in ["peanutLabs"]:
            check_sum = hashlib.md5((user_id + survey['appId'] + survey['secretkey']).encode('utf-8'))
            peanut_id = user_id + "-" + survey['appId'] + "-" + check_sum.hexdigest()[:10]
            entry_url = f"{survey['url']}?userId={peanut_id}&sub_id={transaction_id}"

            return entry_url

        elif buyer_name in ["dynata"]:
            check_sum = hashlib.md5((user_id + survey['appId'] + survey['secretkey']).encode('utf-8'))
            peanut_id = user_id + "-" + survey['appId'] + "-" + check_sum.hexdigest()[:10]
            entry_url = f"{survey['url']}?pub_id={survey['appId']}&sub_id={transaction_id}&user_id={peanut_id}"

            return entry_url

        elif buyer_name in ["lucid"]:
            try:
                country_code = profile['lucidLocale']
            except Exception as e:
                print(e)
                country_code = 0

            url = survey['url'] + "&sid=" + str(survey['appId']) + '&pid=' + user_id \
                  + '&clid=' + str(country_code) + '&mid=' + transaction_id + '&'

            encoded_key = survey['secretkey'].encode('utf-8')
            encoded_url = url.encode('utf-8')
            hashed = hmac.new(encoded_key, msg=encoded_url, digestmod=hashlib.sha1)
            digested_hash = hashed.digest()
            base64_encoded_result = base64.b64encode(digested_hash)
            final_result = base64_encoded_result.decode('utf-8').replace('+', '-').replace('/', '_').replace('=', '')

            entry_url = url + 'hash=' + final_result

            return entry_url
