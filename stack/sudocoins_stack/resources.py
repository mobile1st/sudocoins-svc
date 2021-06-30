import typing

from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_apigatewayv2_authorizers as api_authorizers,
    aws_cognito as cognito,
    aws_sns as sns,
    aws_sqs as sqs,
    aws_apigatewayv2 as apigwv2,
    aws_route53 as route53,
    aws_route53_targets as route53_targets,
    aws_certificatemanager as acm
)


class SudocoinsImportedResources:
    def grant_read_index_data(self, function: _lambda.Function, tables: typing.Sequence[dynamodb.Table]):
        indexes = [f'{table.table_arn}/index/*' for table in tables]
        function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=indexes,
                actions=['dynamodb:Query']
            )
        )

    def __init__(self, scope: cdk.Construct):
        self.sudocoins_domain_name = self.custom_domain(scope)
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
        self.contact_table = dynamodb.Table.from_table_arn(
            scope,
            'ContactTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/Contact'
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
        self.verifications_table = dynamodb.Table.from_table_arn(
            scope,
            'VerificationsTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/Verifications'
        )
        self.art_votes_table = dynamodb.Table.from_table_arn(
            scope,
            'ArtVotesTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/art_votes'
        )
        self.sudocoins_admin_authorizer = self.init_admin_authorizer(scope)
        self.sudocoins_authorizer = self.init_authorizer(scope)
        self.transaction_topic = sns.Topic.from_topic_arn(
            scope,
            'TransactionTopic',
            topic_arn='arn:aws:sns:us-west-2:977566059069:transaction-event'
        )
        self.end_transaction_queue = sqs.Queue.from_queue_arn(
            scope,
            'EndTransactionQueue',
            'arn:aws:sqs:us-west-2:977566059069:EndTransaction.fifo'
        )
        self.art_counter_queue = sqs.Queue(
            scope,
            'ArtViewCounterQueue',
            queue_name='ArtViewCounterQueue.fifo',
            fifo=True,
            content_based_deduplication=True
        )
        self.affiliates_queue = sqs.Queue(
            scope,
            'affiliates.fifo',
            queue_name='affiliates.fifo',
            fifo=True,
            content_based_deduplication=True
        )
        self.add_art_queue = sqs.Queue(
            scope,
            'add_art.fifo',
            queue_name='add_art.fifo',
            fifo=True,
            content_based_deduplication=True
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

    def custom_domain(self, scope: cdk.Construct):
        hosted_zone = route53.HostedZone.from_hosted_zone_attributes(
            scope,
            'SudocoinsZone',
            hosted_zone_id='Z078942423UGVMS8LO8GZ',
            zone_name='sudocoins.com'
        )
        certificate = acm.Certificate.from_certificate_arn(
            scope,
            'SudocoinsApiCertificate',
            'arn:aws:acm:us-west-2:977566059069:certificate/ebe1b64b-4806-4b92-a4be-83372ccbe724'
        )
        domain_name = apigwv2.DomainName(
            scope,
            'SudocoinsDomainName',
            domain_name='app.sudocoins.com',
            certificate=certificate
        )
        target = route53_targets.ApiGatewayv2DomainProperties(
            domain_name.regional_domain_name,
            domain_name.regional_hosted_zone_id
        )
        route53.ARecord(
            scope,
            'SudocoinsApiARecord',
            zone=hosted_zone,
            record_name='app.sudocoins.com',
            target=route53.RecordTarget.from_alias(target)
        )
        return domain_name
