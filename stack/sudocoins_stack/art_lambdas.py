from resources import SudocoinsImportedResources
from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_logs as logs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_iam as iam
)

lambda_default_kwargs = {
    'runtime': _lambda.Runtime.PYTHON_3_8,
    'code': _lambda.Code.asset('../src'),
    'memory_size': 512,
    'log_retention': logs.RetentionDays.THREE_MONTHS
}


class SudocoinsArtLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        # ADD ART
        self.add_art_function = _lambda.Function(
            scope,
            'ArtAddV2',
            function_name='ArtAddV2',
            handler='art.add_art.lambda_handler',
            timeout=cdk.Duration.seconds(5),
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.add_art_function)
        resources.art_uploads_table.grant_read_write_data(self.add_art_function)
        resources.creators_table.grant_read_write_data(self.add_art_function)
        resources.art_processor_topic.grant_publish(self.add_art_function)
        resources.profile_table.grant_read_write_data(self.add_art_function)
        resources.ledger_table.grant_read_write_data(self.add_art_function)
        resources.grant_read_index_data(
            self.add_art_function,
            [resources.art_table, resources.art_uploads_table, resources.ledger_table]
        )
        # INCREMENT VIEW COUNT
        self.increment_view_count_function = _lambda.Function(
            scope,
            'ArtIncrementViewCountV2',
            function_name='ArtIncrementViewCountV2',
            handler='art.increment_view_count.lambda_handler',
            **lambda_default_kwargs
        )
        #  resources.art_counter_queue.grant_send_messages(self.increment_view_count_function)
        # GET SHARED ART
        self.get_shared_art_function = _lambda.Function(
            scope,
            'ArtGetSharedArtV2',
            function_name='ArtGetSharedArtV2',
            handler='art.get_shared_art.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_data(self.get_shared_art_function)
        resources.art_uploads_table.grant_read_data(self.get_shared_art_function)
        #  resources.art_counter_queue.grant_send_messages(self.get_shared_art_function)
        resources.grant_read_index_data(self.get_shared_art_function, [resources.art_table])
        # ART SOURCE REDIRECT
        self.art_source_redirect_function = _lambda.Function(
            scope,
            'ArtSourceRedirectV2',
            function_name='ArtSourceRedirectV2',
            handler='art.art_source_redirect.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.art_source_redirect_function)
        resources.art_uploads_table.grant_read_write_data(self.art_source_redirect_function)
        # GET ARTS
        self.get_arts_function = _lambda.Function(
            scope,
            'ArtBatchGetV2',
            function_name='ArtBatchGetV2',
            handler='art.get_arts.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_data(self.get_arts_function)
        # GET RECENT
        self.get_recent_function = _lambda.Function(
            scope,
            'ArtGetRecentV2',
            function_name='ArtGetRecentV2',
            handler='art.lists.get_recent.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_data(self.get_recent_function)
        resources.grant_read_index_data(self.get_recent_function, [resources.art_table])
        # GET TRENDING
        self.get_trending_function = _lambda.Function(
            scope,
            'ArtGetTrendingV2',
            function_name='ArtGetTrendingV2',
            handler='art.lists.get_trending.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_data(self.get_trending_function)
        # SET TRENDING
        set_trending_function = _lambda.Function(
            scope,
            'ArtSetTrendingV2',
            function_name='ArtSetTrendingV2',
            handler='art.set_lists.set_trending.lambda_handler',
            timeout=cdk.Duration.seconds(420),
            memory_size=4800,
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('../src'),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        resources.art_table.grant_read_data(set_trending_function)
        resources.config_table.grant_read_write_data(set_trending_function)
        resources.grant_read_index_data(set_trending_function, [resources.art_table])
        set_trending_schedule = events.Schedule.rate(cdk.Duration.minutes(10))
        set_trending_target = events_targets.LambdaFunction(handler=set_trending_function)
        events.Rule(
            scope,
            "SetTrendingRule",
            description="Periodically refreshes trending arts sorted by click counts",
            enabled=True,
            schedule=set_trending_schedule,
            targets=[set_trending_target]
        )
        # GET USER ARTS
        self.get_user_arts_function = _lambda.Function(
            scope,
            'ArtGetUserArtsV2',
            function_name='ArtGetUserArtsV2',
            handler='art.lists.get_user_arts.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_uploads_table.grant_read_data(self.get_user_arts_function)
        resources.grant_read_index_data(self.get_user_arts_function, [resources.art_table, resources.art_uploads_table])
        resources.art_table.grant_read_data(self.get_user_arts_function)
        # GET PREVIEW
        self.get_preview_function = _lambda.Function(
            scope,
            'ArtGetPreviewV2',
            function_name='ArtGetPreviewV2',
            handler='art.get_preview.lambda_handler',
            timeout=cdk.Duration.seconds(5),
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.get_preview_function)
        resources.art_uploads_table.grant_read_write_data(self.get_preview_function)
        resources.grant_read_index_data(self.get_preview_function, [resources.art_table, resources.art_uploads_table])
        # ART SEARCH
        self.art_search_function = _lambda.Function(
            scope,
            'ArtSearch',
            function_name='ArtSearch',
            handler='search.art_search.lambda_handler',
            timeout=cdk.Duration.seconds(10),
            **lambda_default_kwargs
        )
        resources.search_table.grant_read_write_data(self.art_search_function)
        resources.art_table.grant_read_write_data(self.art_search_function)
        resources.grant_read_index_data(self.art_search_function, [resources.art_table])
        resources.collections_table.grant_read_write_data(self.art_search_function)
        # SITEMAP UPLOADER
        sitemap_uploader_function = _lambda.Function(
            scope,
            'ArtSitemapUploader',
            function_name='ArtSitemapUploader',
            handler='search.sitemap_uploader.lambda_handler',
            timeout=cdk.Duration.minutes(15),
            **lambda_default_kwargs
        )
        events.Rule(
            scope,
            'ArtSitemapUploaderRule',
            rule_name='ArtSitemapUploaderRule',
            enabled=True,
            schedule=events.Schedule.cron(minute='0', hour='4', day='*', month='*', year='*'),
            targets=[events_targets.LambdaFunction(handler=sitemap_uploader_function)]
        )
        resources.art_table.grant_read_write_data(sitemap_uploader_function)
        resources.grant_read_index_data(sitemap_uploader_function, [resources.art_table])
        resources.sitemaps_bucket.grant_read_write(sitemap_uploader_function)
        # UPDATE ART TAGS
        self.update_tags_function = _lambda.Function(
            scope,
            'UpdateTagsV2',
            function_name='UpdateTagsV2',
            handler='art.update_tags.lambda_handler',
            timeout=cdk.Duration.seconds(5),
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.update_tags_function)
        # INGEST OPENSEA
        ingest_opensea_function = _lambda.Function(
            scope,
            'IngestOpenSeaV2',
            function_name='IngestOpenSeaV2',
            timeout=cdk.Duration.seconds(30),
            handler='art.events.ingest_opensea.lambda_handler',
            **lambda_default_kwargs
        )
        ingest_opensea_schedule = events.Schedule.rate(cdk.Duration.minutes(2))
        ingest_opensea_target = events_targets.LambdaFunction(handler=ingest_opensea_function)
        events.Rule(
             scope,
             "IngestOpenseaRule",
             description="Periodically refreshes trending arts sorted by click counts",
             enabled=True,
             schedule=ingest_opensea_schedule,
             targets=[ingest_opensea_target]
         )
        resources.ingest_opensea_topic.grant_publish(ingest_opensea_function)
        resources.art_table.grant_read_write_data(ingest_opensea_function)
        resources.config_table.grant_read_write_data(ingest_opensea_function)
        resources.grant_read_index_data(
            ingest_opensea_function,
            [resources.art_table]
        )
        # INGEST PROCESSOR
        ingest_processor_function = _lambda.Function(
            scope,
            'IngestProcessorV2',
            function_name='IngestProcessorV2',
            timeout=cdk.Duration.seconds(30),
            handler='art.events.ingest_processor.lambda_handler',
            **lambda_default_kwargs
        )
        resources.ingest_opensea_topic.add_subscription(
            subs.LambdaSubscription(
                ingest_processor_function
            )
        )
        resources.art_table.grant_read_write_data(ingest_processor_function)
        resources.grant_read_index_data(
            ingest_processor_function,
            [resources.art_table]
        )
        resources.art_processor_topic.grant_publish(ingest_processor_function)
        resources.add_search_topic.grant_publish(ingest_processor_function)
        resources.add_time_series_topic.grant_publish(ingest_processor_function)
        resources.creators_table.grant_read_write_data(ingest_processor_function)
        resources.collections_table.grant_read_write_data(ingest_processor_function)
        # GET HEARTS
        self.get_hearts_function = _lambda.Function(
            scope,
            'GetHeartsV2',
            function_name='GetHeartsV2',
            handler='art.get_hearts.lambda_handler',
            **lambda_default_kwargs
        )
        # AUTO TWEET
        self.auto_tweet_function = _lambda.Function(
            scope,
            'AutoTweetV2',
            function_name='AutoTweetV2',
            handler='art.marketing.auto_tweet.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(self.auto_tweet_function)
        resources.art_table.grant_read_write_data(self.auto_tweet_function)
        resources.auto_tweet_table.grant_read_write_data(self.auto_tweet_function)

        auto_tweet_schedule = events.Schedule.rate(cdk.Duration.minutes(60))
        auto_tweet_target = events_targets.LambdaFunction(handler=self.auto_tweet_function)
        events.Rule(
            scope,
            "AutoTweetRule",
            description="Periodically tweets trending arts",
            enabled=True,
            schedule=auto_tweet_schedule,
            targets=[auto_tweet_target]
        )

        # AUTO FB
        self.auto_fb_function = _lambda.Function(
            scope,
            'AutoFBV2',
            function_name='AutoFBV2',
            handler='art.marketing.auto_fb.lambda_handler',
            timeout=cdk.Duration.seconds(8),
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(self.auto_fb_function)
        resources.art_table.grant_read_write_data(self.auto_fb_function)
        resources.auto_tweet_table.grant_read_write_data(self.auto_fb_function)
        auto_fb_schedule = events.Schedule.rate(cdk.Duration.minutes(1440))
        auto_fb_target = events_targets.LambdaFunction(handler=self.auto_fb_function)
        events.Rule(
            scope,
            "AutoFBRule",
            description="Periodically posts trending arts",
            enabled=True,
            schedule=auto_fb_schedule,
            targets=[auto_fb_target]
        )
        # START MINT
        self.start_mint_function = _lambda.Function(
            scope,
            'StartMintV2',
            function_name='StartMintV2',
            handler='art.minting.start_mint.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_bucket.grant_read_write(self.start_mint_function)
        resources.art_bucket.grant_put(self.start_mint_function)
        resources.art_bucket.grant_put_acl(self.start_mint_function)
        resources.art_table.grant_read_write_data(self.start_mint_function)
        # SET IPFS
        self.set_ipfs_function = _lambda.Function(
            scope,
            'SetIPFSV2',
            function_name='SetIPFSV2',
            handler='art.minting.set_ipfs.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_bucket.grant_read_write(self.set_ipfs_function)
        # GET MINT REQUEST
        self.get_mint_function = _lambda.Function(
            scope,
            'GetMintRequestV2',
            function_name='GetMintRequestV2',
            handler='art.minting.get_mint_request.lambda_handler',
            **lambda_default_kwargs
        )
        # END MINT REQUEST
        self.end_mint_function = _lambda.Function(
            scope,
            'EndMintV2',
            function_name='EndMintV2',
            handler='art.minting.end_mint.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.end_mint_function)
        resources.collections_table.grant_read_write_data(self.end_mint_function)
        # ADD SEARCH
        add_search_function = _lambda.Function(
            scope,
            'AddSearchV2',
            function_name='AddSearchV2',
            handler='art.add_search.lambda_handler',
            **lambda_default_kwargs
        )
        resources.add_search_topic.add_subscription(
            subs.LambdaSubscription(
                add_search_function
            )
        )
        resources.search_table.grant_read_write_data(add_search_function)
        resources.art_bucket.grant_read_write(add_search_function)
        # COLLECTION NAME
        self.collection_name_function = _lambda.Function(
            scope,
            'CollectionNameV2',
            function_name='CollectionNameV2',
            handler='art.get_collection_name.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.collection_name_function)
        resources.collections_table.grant_read_write_data(self.collection_name_function)
        resources.grant_read_index_data(
            self.collection_name_function,
            [resources.art_table]
        )
        # DELETE ART
        self.delete_art_function = _lambda.Function(
            scope,
            'DeleteArtV2',
            function_name='DeleteArtV2',
            handler='art.delete_art.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.delete_art_function)
        resources.sub_table.grant_read_data(self.delete_art_function)
        resources.grant_read_index_data(
            self.delete_art_function,
            [resources.art_table]
        )
        # GET MINTED
        self.get_minted_function = _lambda.Function(
            scope,
            'GetMintedV2',
            function_name='GetMintedV2',
            handler='art.lists.get_minted.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.get_minted_function)
        resources.grant_read_index_data(
            self.get_minted_function,
            [resources.art_table]
        )
        # ADD TIME SERIES
        self.add_time_series_function = _lambda.Function(
            scope,
            'AddTimeSeriesV2',
            function_name='AddTimeSeriesV2',
            handler='art.events.add_time_series.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.get_minted_function)
        resources.grant_read_index_data(
            self.get_minted_function,
            [resources.art_table]
        )
        resources.add_time_series_topic.add_subscription(
            subs.LambdaSubscription(
                self.add_time_series_function
            )
        )
        resources.time_series_table.grant_read_write_data(self.add_time_series_function)
        resources.collections_table.grant_read_write_data(self.add_time_series_function)
        # COLLECTION PAGE
        self.collection_page_function = _lambda.Function(
            scope,
            'CollectionPageV2',
            function_name='CollectionPageV2',
            handler='art.lists.collection_page.lambda_handler',
            timeout=cdk.Duration.seconds(15),
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.collection_page_function)
        resources.grant_read_index_data(
            self.collection_page_function,
            [resources.art_table]
        )
        # SET COLLECTION TIME SERIES
        self.set_coll_ts_function = _lambda.Function(
            scope,
            'SetCollectionTSV2',
            function_name='SetCollectionTS',
            handler='art.set_lists.set_collection_ts.lambda_handler',
            timeout=cdk.Duration.seconds(60),
            **lambda_default_kwargs
        )
        resources.time_series_table.grant_read_write_data(self.set_coll_ts_function)
        resources.config_table.grant_read_write_data(self.set_coll_ts_function)
        set_collection_ts_schedule = events.Schedule.rate(cdk.Duration.minutes(10))
        set_collection_ts_target = events_targets.LambdaFunction(handler=self.set_coll_ts_function)
        events.Rule(
            scope,
            "SetCollectionTSRule",
            description="Periodically sets collection time series data",
            enabled=True,
            schedule=set_collection_ts_schedule,
            targets=[set_collection_ts_target]
        )
        # TOP BUYERS PAGE
        self.top_buyers_page_function = _lambda.Function(
            scope,
            'TopBuyersPageV2',
            function_name='TopBuyersPageV2',
            handler='art.lists.top_buyers_page.lambda_handler',
            timeout=cdk.Duration.seconds(15),
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.top_buyers_page_function)
        resources.grant_read_index_data(
            self.top_buyers_page_function,
            [resources.art_table]
        )
        # GET BUYERS
        self.get_buyers_function = _lambda.Function(
            scope,
            'GetBuyersV2',
            function_name='GetBuyersV2',
            handler='art.lists.get_buyers.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_data(self.get_buyers_function)
        # GET CHART DATA
        self.get_chart_data_function = _lambda.Function(
            scope,
            'GetChartDataV2',
            function_name='GetChartDataV2',
            handler='art.get_chart_data.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_data(self.get_chart_data_function)
        # GET META MASK
        get_meta_mask_function = _lambda.Function(
            scope,
            'GetMetaMaskV2',
            function_name='GetMetaMaskV2',
            handler='art.get_mm_arts.lambda_handler',
            timeout=cdk.Duration.seconds(300),
            **lambda_default_kwargs
        )
        resources.get_meta_mask_topic.add_subscription(
            subs.LambdaSubscription(
                get_meta_mask_function
            )
        )
        resources.art_table.grant_read_write_data(get_meta_mask_function)
        resources.grant_read_index_data(
            get_meta_mask_function,
            [resources.art_table]
        )
        resources.art_processor_topic.grant_publish(get_meta_mask_function)
        # SET NEW COLLECTIONS
        set_new_collections_function = _lambda.Function(
            scope,
            'SetNewCollectionsV2',
            function_name='SetNewCollectionsV2',
            handler='art.set_lists.set_new_collections.lambda_handler',
            timeout=cdk.Duration.seconds(60),
            **lambda_default_kwargs
        )
        resources.collections_table.grant_read_data(set_new_collections_function)
        resources.config_table.grant_read_write_data(set_new_collections_function)
        resources.grant_read_index_data(set_new_collections_function, [resources.collections_table])
        set_new_collections_schedule = events.Schedule.rate(cdk.Duration.minutes(10))
        set_new_collections_target = events_targets.LambdaFunction(handler=set_new_collections_function)
        events.Rule(
            scope,
            "SetNewCollectionsRule",
            description="Periodically sets the list of new collections",
            enabled=True,
            schedule=set_new_collections_schedule,
            targets=[set_new_collections_target]
        )
        # GET NEW COLLECTIONS
        self.get_new_collections_function = _lambda.Function(
            scope,
            'GetNewCollectionsV2',
            function_name='GetNewCollectionsV2',
            handler='art.lists.get_new_collections.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_data(self.get_new_collections_function)
        # ADD CHAT
        self.add_chat_function = _lambda.Function(
            scope,
            'AddChatV2',
            function_name='AddChatV2',
            handler='art.chat.add_chat.lambda_handler',
            **lambda_default_kwargs
        )
        resources.chat_table.grant_read_write_data(self.add_chat_function)
        resources.chat_connections_table.grant_read_write_data(self.add_chat_function)
        # GET CHATS
        self.get_chats_function = _lambda.Function(
            scope,
            'GetChatsV2',
            function_name='GetChatsV2',
            handler='art.chat.get_chats.lambda_handler',
            **lambda_default_kwargs
        )
        resources.chat_table.grant_read_write_data(self.add_chat_function)
        resources.grant_read_index_data(self.get_chats_function, [resources.chat_table])
        # MANAGE CHAT CONNECTIONS
        self.manage_connections_function = _lambda.Function(
            scope,
            'ManageConnectionsV2',
            function_name='ManageConnectionsV2',
            handler='art.chat.manage_connection.lambda_handler',
            **lambda_default_kwargs
        )
        resources.chat_connections_table.grant_read_write_data(self.manage_connections_function)
        # GET UPCOMING
        self.get_upcoming_function = _lambda.Function(
            scope,
            'GetUpcomingV2',
            function_name='GetUpcomingV2',
            handler='art.lists.get_upcoming.lambda_handler',
            **lambda_default_kwargs
        )
        resources.upcoming_table.grant_read_write_data(self.get_upcoming_function)
        resources.grant_read_index_data(self.get_upcoming_function, [resources.upcoming_table])
        # ADD UPCOMING
        self.add_upcoming_function = _lambda.Function(
            scope,
            'AddUpcomingV2',
            function_name='AddUpcomingV2',
            handler='art.set_lists.add_upcoming.lambda_handler',
            **lambda_default_kwargs
        )
        resources.upcoming_table.grant_read_write_data(self.add_upcoming_function)
        resources.grant_read_index_data(self.add_upcoming_function, [resources.upcoming_table])
        # GET RELATED ART
        self.get_related_function = _lambda.Function(
            scope,
            'GetRelatedArt',
            function_name='GetRelatedArt',
            handler='art.lists.get_related_art.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.get_related_function)
        resources.grant_read_index_data(self.get_related_function, [resources.art_table])
        # SET TOP COLLECTIONS
        set_top_collections_function = _lambda.Function(
            scope,
            'SetTopCollectionsV2',
            function_name='SetTopCollectionsV2',
            handler='art.set_lists.set_top_collections.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            timeout=cdk.Duration.seconds(30),
            code=_lambda.Code.asset('../src'),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        resources.collections_table.grant_read_data(set_top_collections_function)
        resources.config_table.grant_read_write_data(set_top_collections_function)
        set_top_collections_schedule = events.Schedule.rate(cdk.Duration.minutes(10))
        set_top_collection_target = events_targets.LambdaFunction(handler=set_top_collections_function)
        events.Rule(
            scope,
            "SetTopCollectionRule",
            description="Periodically refreshes top collections sorted by volume",
            enabled=True,
            schedule=set_top_collections_schedule,
            targets=[set_top_collection_target]
        )
        # GET TOP COLLECTIONS
        self.get_top_collections_function = _lambda.Function(
            scope,
            'GetTopCollections',
            function_name='GetTopCollections',
            handler='art.lists.get_top_collections.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(self.get_top_collections_function)
        # GET TRADES DELTA
        self.get_trades_delta_function = _lambda.Function(
            scope,
            'GetTradesDelta',
            function_name='GetTradesDelta',
            timeout=cdk.Duration.seconds(30),
            handler='art.lists.get_trades_delta.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(self.get_trades_delta_function)
        # SET TRADES DELTA
        set_trades_delta_function = _lambda.Function(
            scope,
            'SetTradesDelta',
            function_name='SetTradesDelta',
            handler='art.set_lists.trades_delta.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('../src'),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        resources.collections_table.grant_read_data(set_trades_delta_function)
        resources.config_table.grant_read_write_data(set_trades_delta_function)
        set_trades_delta_schedule = events.Schedule.rate(cdk.Duration.minutes(2))
        set_trades_delta_target = events_targets.LambdaFunction(handler=set_trades_delta_function)
        events.Rule(
            scope,
            "SetTradesDeltaRule",
            description="Periodically sets top trades delta",
            enabled=True,
            schedule=set_trades_delta_schedule,
            targets=[set_trades_delta_target]
        )
        # INGEST OPENSEA2
        ingest_opensea2_function = _lambda.Function(
            scope,
            'IngestOpenSea2V2',
            function_name='IngestOpenSea2V2',
            timeout=cdk.Duration.seconds(30),
            handler='art.events.ingest_opensea2.lambda_handler',
            **lambda_default_kwargs
        )
        """
        ingest_opensea2_schedule = events.Schedule.rate(cdk.Duration.minutes(2))
        ingest_opensea2_target = events_targets.LambdaFunction(handler=ingest_opensea2_function)
        events.Rule(
            scope,
            "IngestOpensea2Rule",
            description="Periodically refreshes trending arts sorted by click counts",
            enabled=True,
            schedule=ingest_opensea2_schedule,
            targets=[ingest_opensea2_target]
        )
        """
        resources.ingest_opensea2_topic.grant_publish(ingest_opensea2_function)
        resources.art_table.grant_read_write_data(ingest_opensea2_function)
        resources.config_table.grant_read_write_data(ingest_opensea2_function)
        resources.grant_read_index_data(
            ingest_opensea2_function,
            [resources.art_table]
        )
        # INGEST PROCESSOR2
        ingest_processor2_function = _lambda.Function(
            scope,
            'IngestProcessor2V2',
            function_name='IngestProcessor2V2',
            timeout=cdk.Duration.seconds(30),
            handler='art.events.ingest_processor2.lambda_handler',
            **lambda_default_kwargs
        )
        resources.ingest_opensea2_topic.add_subscription(
            subs.LambdaSubscription(
                ingest_processor2_function
            )
        )
        resources.art_table.grant_read_write_data(ingest_processor2_function)
        resources.grant_read_index_data(
            ingest_processor2_function,
            [resources.art_table]
        )
        resources.add_search_topic.grant_publish(ingest_processor2_function)
        resources.add_time_series_topic.grant_publish(ingest_processor2_function)
        resources.creators_table.grant_read_write_data(ingest_processor2_function)
        resources.collections_table.grant_read_write_data(ingest_processor2_function)









