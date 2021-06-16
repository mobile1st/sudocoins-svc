from unused import sudocoins_logger
from art.history import History
from datetime import datetime
from survey.rev_shares import RevenueData
import json

log = sudocoins_logger.get()


class Transaction:

    def __init__(self, dynamodb, sns_client):
        self.dynamodb = dynamodb
        self.sns_client = sns_client
        self.history = History(self.dynamodb)
        self.rev_data = RevenueData(self.dynamodb)

    def end(self, data):
        timestamp = str(datetime.utcnow().isoformat())
        buyer_name = data['buyerName']

        if buyer_name == 'lucid':
            transaction_id = data['transactionId']
            status = data["status"]
            user_id = data["userId"]

        elif buyer_name == 'cint' or buyer_name == 'test':
            transaction_id = data["queryStringParameters"]['sid']
            status = data["queryStringParameters"]['status']

            transaction_table = self.dynamodb.Table('Transaction')
            transaction_item = transaction_table.get_item(Key={'transactionId': transaction_id})
            user_id = transaction_item['Item']['userId']

        else:
            log.warn(f'Transaction.end - Unsupported buyer={buyer_name} data={data}')
            return

        revenue, payment, user_status, rev_share, cut = self.rev_data.get_rev_share(data, buyer_name)
        log.info(f'Transaction.end buyer={buyer_name} transactionId={transaction_id} status={status} userId={user_id}'
                 f' userStatus={user_status} revenue={revenue} share={rev_share} cut={cut} payment={payment} data={data}')

        self.history.updateTransaction(transaction_id, payment, status, timestamp,
                                       revenue, rev_share, user_status, cut, data, user_id)

        if payment > 0:
            self.history.createLedgerRecord(transaction_id, payment, user_id, timestamp, user_status)

        pub_status = 'C' if status == 'success' else status
        self.sns_client.publish(
            TopicArn="arn:aws:sns:us-west-2:977566059069:transaction-event",
            MessageStructure='string',
            Message=json.dumps({
                "source": 'SURVEY_END',
                "status": pub_status,
                "transactionId": transaction_id,
                "timestamp": timestamp,
                "buyerName": buyer_name,
                "userId": user_id,
                "revenue": str(revenue)  # Decimal is not JSON serializable
            })
        )