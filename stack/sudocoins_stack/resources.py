from aws_cdk import (
    core as cdk,
    aws_dynamodb as dynamodb,
    aws_apigatewayv2_authorizers as api_authorizers,
    aws_cognito as cognito,
    aws_sns as sns,
    aws_sqs as sqs
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
        self.config_table = dynamodb.Table.from_table_arn(
            scope,
            'ConfigTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/Config'
        )
        self.profile_table = dynamodb.Table.from_table_arn(
            scope,
            'ProfileTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/Profile'
        )
        self.sub_table = dynamodb.Table.from_table_arn(
            scope,
            'SubTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/sub'
        )
        self.art_table = dynamodb.Table.from_table_arn(
            scope,
            'ArtTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/art'
        )
        self.art_uploads_table = dynamodb.Table.from_table_arn(
            scope,
            'ArtUploadsTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/art_uploads'
        )
        self.sudocoins_admin_authorizer = self.init_admin_authorizer(scope)
        self.sudocoins_authorizer = self.init_authorizer(scope)
        self.transaction_topic = sns.Topic.from_topic_arn(
            scope,
            "TransactionTopic",
            topic_arn='arn:aws:sns:us-west-2:977566059069:transaction-event'
        )
        self.end_transaction_queue = sqs.Queue.from_queue_arn(
            scope,
            'EndTransactionQueue',
            'arn:aws:sqs:us-west-2:977566059069:EndTransaction.fifo'
        )
        self.art_counter_queue = sqs.Queue.from_queue_arn(
            scope,
            'ArtCounterQueue',
            'arn:aws:sqs:us-west-2:977566059069:art_counter.fifo'
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

    def init_authorizer(self, scope: cdk.Construct):
        sudocoins_pool = cognito.UserPool.from_user_pool_id(scope, 'Sudocoins-test', 'us-west-2_Z8tutP64m')
        sudocoins_ui_client = cognito.UserPoolClient.from_user_pool_client_id(
            scope,
            'SudocoinsUIClient',
            'u9c3r4mn10c1brn6df3g1kqbr'
        )
        return api_authorizers.HttpUserPoolAuthorizer(
            user_pool=sudocoins_pool,
            user_pool_client=sudocoins_ui_client
        )
