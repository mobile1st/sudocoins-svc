from aws_cdk import (
    core as cdk,
    aws_dynamodb as dynamodb,
    aws_apigatewayv2_authorizers as api_authorizers,
    aws_cognito as cognito,
    aws_sns as sns
)


class SudocoinsImportedResources:
    def __init__(self, scope: cdk.Construct):
        self.traffic_reports_table = dynamodb.Table.from_table_arn(
            scope,
            'TrafficReportsTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/TrafficReports'
        )
        self.payouts_table = dynamodb.Table.from_table_arn(
            scope,
            'PayoutsTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/Payouts'
        )
        self.ledger_table = dynamodb.Table.from_table_arn(
            scope,
            'LedgerTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/Ledger'
        )
        self.transaction_table = dynamodb.Table.from_table_arn(
            scope,
            'TransactionTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/Transaction'
        )
        self.sudocoins_admin_authorizer = self.init_admin_authorizer(scope)
        self.transaction_topic = sns.Topic.from_topic_arn(
            scope,
            "TransactionTopic",
            topic_arn='arn:aws:sns:us-west-2:977566059069:transaction-event'
        )

    def init_admin_authorizer(self, scope: cdk.Construct):
        sudocoins_admin_pool = cognito.UserPool.from_user_pool_id(scope, 'SudoCoins-Admin', 'us-west-2_TpPw8Ed2z')
        sudocoins_admin_ui_client = cognito.UserPoolClient.from_user_pool_client_id(
            scope,
            'SudocoinsAdminUIClient',
            '1emc1ko93cb7priri26dtih1pq'
        )
        return api_authorizers.HttpUserPoolAuthorizer(
            user_pool=sudocoins_admin_pool,
            user_pool_client=sudocoins_admin_ui_client
        )
