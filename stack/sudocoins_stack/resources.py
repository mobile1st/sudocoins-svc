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
        self.import_hosted_zone(scope)
        self.import_tables(scope)
        self.construct_tables(scope)
        self.construct_s3_buckets(scope)
        self.construct_topics(scope)
        self.init_cdn(scope)
        self.sudocoins_domain_name = self.custom_domain(scope)
        self.sudocoins_admin_authorizer = self.init_admin_authorizer(scope)
        self.sudocoins_authorizer = self.init_authorizer(scope)

    def import_hosted_zone(self, scope):
        self.hosted_zone = route53.HostedZone.from_hosted_zone_attributes(
            scope,
            'SudocoinsZone',
            hosted_zone_id='Z078942423UGVMS8LO8GZ',
            zone_name='sudocoins.com'
        )


    def construct_topics(self, scope):

        self.art_processor_topic = sns.Topic(
            scope,
            'ArtProcessorTopic',
            display_name='ArtProcessor',
            topic_name='ArtProcessor'
        )
        self.ingest_opensea2_topic = sns.Topic(
            scope,
            'IngestOpenSea2Topic',
            display_name='IngestOpenSea2Topic',
            topic_name='IngestOpenSea2Topic'
        )
        self.add_search_topic = sns.Topic(
            scope,
            'AddSearchTopic',
            display_name='AddSearchTopic',
            topic_name='AddSearchTopic'
        )
        self.add_time_series2_topic = sns.Topic(
            scope,
            'AddTimeSeries2Topic',
            display_name='AddTimeSeries2Topic',
            topic_name='AddTimeSeries2Topic'
        )
        self.listings_topic = sns.Topic(
            scope,
            'ListingsTopic',
            display_name='ListingsTopic',
            topic_name='ListingsTopic'
        )
        self.add_score_topic = sns.Topic(
            scope,
            'AddScoreTopic',
            display_name='AddScoreTopic',
            topic_name='AddScoreTopic'
        )

    def construct_s3_buckets(self, scope):
        sitemaps_bucket_name = 'sitemaps.sudocoins.com'
        self.sitemaps_bucket = s3.Bucket(
            scope,
            'SitemapsBucket',
            bucket_name=sitemaps_bucket_name,
            public_read_access=True,
            website_index_document='sitemaps.xml'
        )
        route53.ARecord(
            scope,
            'SitemapsBucketARecord',
            zone=self.hosted_zone,
            record_name=sitemaps_bucket_name,
            target=route53.RecordTarget.from_alias(route53_targets.BucketWebsiteTarget(self.sitemaps_bucket))
        )

    def construct_tables(self, scope):

        self.ether_events_table = dynamodb.Table(
            scope,
            'EtherEventsTable',
            table_name='ether_events',
            partition_key=dynamodb.Attribute(name='tx_hash', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )
        self.auto_tweet_table = dynamodb.Table(
            scope,
            'AutoTweetTable',
            table_name='auto_tweet',
            partition_key=dynamodb.Attribute(name='art_id', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )
        self.binance_events_table = dynamodb.Table(
            scope,
            'BinanceEventsTable',
            table_name='binance_events',
            partition_key=dynamodb.Attribute(name='tx_hash', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )
        self.search_table = dynamodb.Table(
            scope,
            'SearchTable',
            table_name='search',
            partition_key=dynamodb.Attribute(name='search_key', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )


    def import_tables(self, scope):
        self.transaction_table = self.import_table(scope, 'Transaction')
        self.config_table = self.import_table(scope, 'Config')
        self.contact_table = self.import_table(scope, 'Contact')
        self.sub_table = self.import_table(scope, 'sub')
        self.art_table = self.import_table(scope, 'art')
        self.ether_events_table = self.import_table(scope, 'ether_events')
        self.binance_events_table = self.import_table(scope, 'binance_events')
        self.search_table = self.import_table(scope, 'search')
        self.collections_table = self.import_table(scope, 'collections')
        self.chat_table = self.import_table(scope, 'chat')
        self.chat_connections_table = self.import_table(scope, 'chat_connections')
        self.portfolio_table = self.import_table(scope, 'portfolio')
        self.news_table = self.import_table(scope, 'news')

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
            cors=[s3.CorsRule(
                allowed_methods=[
                    s3.HttpMethods.HEAD,
                    s3.HttpMethods.GET,
                    s3.HttpMethods.POST,
                    s3.HttpMethods.PUT
                ],
                allowed_origins=['*'],
                allowed_headers=['*']
            )]
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
