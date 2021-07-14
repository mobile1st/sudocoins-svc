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
    aws_certificatemanager as acm,
    aws_s3 as s3,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins
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
        self.import_tables(scope)
        self.sudocoins_domain_name = self.custom_domain(scope)
        self.sudocoins_cdn = self.init_cdn(scope)
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
        self.art_processor_topic = sns.Topic(
            scope,
            'ArtProcessorTopic',
            display_name='ArtProcessor',
            topic_name='ArtProcessor'
        )
        self.affiliates_topic = sns.Topic(
            scope,
            'AffiliateTopic',
            display_name='Affiliates',
            topic_name='Affiliates'
        )
        self.art_processor_bucket = s3.Bucket(
            scope,
            'ArtProcessorBucket',
            bucket_name='art-processor-bucket'
        )

    def import_tables(self, scope):
        self.traffic_reports_table = self.import_table(scope, 'TrafficReports')
        self.payouts_table = self.import_table(scope, 'Payouts')
        self.ledger_table = self.import_table(scope, 'Ledger')
        self.transaction_table = self.import_table(scope, 'Transaction')
        self.config_table = self.import_table(scope, 'Config')
        self.contact_table = self.import_table(scope, 'Contact')
        self.profile_table = self.import_table(scope, 'Profile')
        self.sub_table = self.import_table(scope, 'sub')
        self.art_table = self.import_table(scope, 'art')
        self.art_uploads_table = self.import_table(scope, 'art_uploads')
        self.verifications_table = self.import_table(scope, 'Verifications')
        self.art_votes_table = self.import_table(scope, 'art_votes')
        self.creators_table = self.import_table(scope, 'creators')

    def import_table(self, scope, table_name):
        return dynamodb.Table.from_table_arn(
            scope,
            f'{table_name}Table',
            f'arn:aws:dynamodb:us-west-2:977566059069:table/{table_name}'
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
        self.hosted_zone = route53.HostedZone.from_hosted_zone_attributes(
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
            zone=self.hosted_zone,
            record_name='app.sudocoins.com',
            target=route53.RecordTarget.from_alias(target)
        )
        return domain_name

    def init_cdn(self, scope):
        self.art_bucket = s3.Bucket(
            scope,
            'ArtBucket',
            bucket_name='sudocoins-art-bucket',
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )
        certificate = acm.Certificate.from_certificate_arn(
            scope,
            'SudocoinsCdnCertificate',
            'arn:aws:acm:us-east-1:977566059069:certificate/952a2b92-846a-43c3-80e0-a000483e7643'
        )
        distribution = cloudfront.Distribution(
            scope,
            'ArtDistribution',
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3Origin(self.art_bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS
            ),
            certificate=certificate,
            domain_names=['cdn.sudocoins.com']
        )
        target = route53_targets.CloudFrontTarget(distribution)
        route53.ARecord(
            scope,
            'SudocoinsCdnARecord',
            zone=self.hosted_zone,
            record_name='cdn.sudocoins.com',
            target=route53.RecordTarget.from_alias(target)
        )
        route53.AaaaRecord(
            scope,
            'SudocoinsCdnAAAARecord',
            zone=self.hosted_zone,
            record_name='cdn.sudocoins.com',
            target=route53.RecordTarget.from_alias(target)
        )
