import requests
import datetime
import hmac
import hashlib
import json


class Wyre:

    def __init__(self):
        self.apiKey = 'AK-WYHM7TZZ-36WCYE2N-YC7NG7UQ-8DLZJ3EF'
        self.token = 'SK-L4ZV42YW-EXWZ762A-CW4URDJB-DTUXA24D'
        self.baseUrl = 'https://api.testwyre.com/v2/'


    def getEpoch(self):

        return str(round(datetime.datetime.utcnow().timestamp() * 1000))


    def generateToken(self, apiKey, url, payload, secret):
        newUrl = url + payload
        token = self.getHash(newUrl, secret)

        return token


    def getHash(self, url, secret):
        string_bytes = (bytearray(url, "utf8"))

        return hmac.new(bytearray(secret, "utf8"), string_bytes, hashlib.sha256).hexdigest()


    def createWallet(self, body):
        path = 'wallets'
        timestamp = self.getEpoch()
        url = self.url + path + "?" + 'timestamp=' + timestamp
        payload = json.dumps(body)
        token = self.generateToken(self.apiKey, url, payload, self.token)

        headers = {
            'X-Api-Signature': token,
            'Content-Type': 'application/json',
            'X-API-Key': apiKey
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        return response











print(response.text.encode('utf8'))
