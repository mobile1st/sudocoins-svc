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
        resources.grant_read_index_data(self.get_preview_function, [resources.art_table])
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
            [resources.art_table, resources.collections_table]
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
        resources.collections_table.grant_read_write_data(self.collection_page_function)
        resources.grant_read_index_data(
            self.collection_page_function,
            [resources.art_table, resources.collections_table]
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
        set_trades_delta_schedule = events.Schedule.rate(cdk.Duration.minutes(10))
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
            'IngestOpenSeaBackground',
            function_name='IngestOpenSeaBackground',
            timeout=cdk.Duration.seconds(30),
            handler='art.events.ingest_opensea2.lambda_handler',
            **lambda_default_kwargs
        )

        ingest_opensea2_schedule = events.Schedule.rate(cdk.Duration.minutes(1))
        ingest_opensea2_target = events_targets.LambdaFunction(handler=ingest_opensea2_function)
        events.Rule(
            scope,
            "IngestOpensea2Rule",
            description="Periodically refreshes trending arts sorted by click counts",
            enabled=True,
            schedule=ingest_opensea2_schedule,
            targets=[ingest_opensea2_target]
        )

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
            'IngestProcessorBackground',
            function_name='IngestProcessorBackground',
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
        resources.add_time_series2_topic.grant_publish(ingest_processor2_function)
        resources.collections_table.grant_read_write_data(ingest_processor2_function)
        resources.art_processor_topic.grant_publish(ingest_processor2_function)
        # ADD TIME SERIES BACKGROUND
        self.add_time_series2_function = _lambda.Function(
            scope,
            'AddTimeSeriesBackground',
            function_name='AddTimeSeriesBackground',
            handler='art.events.add_time_series_2.lambda_handler',
            **lambda_default_kwargs
        )
        resources.add_time_series2_topic.add_subscription(
            subs.LambdaSubscription(
                self.add_time_series2_function
            )
        )
        resources.collections_table.grant_read_write_data(self.add_time_series2_function)
        resources.add_score_topic.grant_publish(self.add_time_series2_function)
        # SET SUDO_INDEX
        sudo_index_function = _lambda.Function(
            scope,
            'SudoIndex',
            function_name='SudoIndex',
            timeout=cdk.Duration.seconds(30),
            handler='art.set_lists.sudo_index.lambda_handler',
            **lambda_default_kwargs
        )

        sudo_index_schedule = events.Schedule.rate(cdk.Duration.minutes(60))
        sudo_index_target = events_targets.LambdaFunction(handler=sudo_index_function)
        events.Rule(
            scope,
            "SudoIndexRule",
            description="Periodically sets Sudo Index",
            enabled=True,
            schedule=sudo_index_schedule,
            targets=[sudo_index_target]
        )

        resources.config_table.grant_read_write_data(sudo_index_function)
        # SET TOP NFTS
        set_top_nfts_function = _lambda.Function(
            scope,
            'SetTopNFTS',
            function_name='SetTopNFTS',
            handler='art.set_lists.set_top_nfts.lambda_handler',
            timeout=cdk.Duration.seconds(420),
            memory_size=4800,
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('../src'),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        resources.config_table.grant_read_write_data(set_top_nfts_function)
        resources.grant_read_index_data(set_top_nfts_function, [resources.art_table])
        set_top_nfts_schedule = events.Schedule.rate(cdk.Duration.minutes(10))
        set_top_nfts_target = events_targets.LambdaFunction(handler=set_top_nfts_function)
        events.Rule(
            scope,
            "SetTopNFTs",
            description="Periodically refreshes trending nfts",
            enabled=True,
            schedule=set_top_nfts_schedule,
            targets=[set_top_nfts_target]
        )
        # READ RSS
        read_rss_function = _lambda.Function(
            scope,
            'ReadRSS',
            function_name='ReadRSS',
            timeout=cdk.Duration.seconds(30),
            handler='art.set_lists.read_rss.lambda_handler',
            **lambda_default_kwargs
        )
        resources.news_table.grant_read_write_data(read_rss_function)
        rss_feed_schedule = events.Schedule.rate(cdk.Duration.minutes(30))
        rss_feed_target = events_targets.LambdaFunction(handler=read_rss_function)
        events.Rule(
            scope,
            "Reed RSS",
            description="Periodically reads rss feeds",
            enabled=True,
            schedule=rss_feed_schedule,
            targets=[rss_feed_target]
        )
        # GET NEWS
        self.get_news_function = _lambda.Function(
            scope,
            'GetNews',
            function_name='GetNews',
            timeout=cdk.Duration.seconds(30),
            handler='art.lists.get_news.lambda_handler',
            **lambda_default_kwargs
        )
        resources.news_table.grant_read_write_data(self.get_news_function)
        resources.grant_read_index_data(self.get_news_function, [resources.news_table])
        # GET Floor DELTA
        self.get_floor_delta_function = _lambda.Function(
            scope,
            'GetFloorDelta',
            function_name='GetFloorDelta',
            timeout=cdk.Duration.seconds(30),
            handler='art.lists.get_floor_delta.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(self.get_floor_delta_function)
        # SET Floor DELTA
        set_floor_delta_function = _lambda.Function(
            scope,
            'SetFloorDelta',
            function_name='SetFloorDelta',
            handler='art.set_lists.floor_delta.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('../src'),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        resources.collections_table.grant_read_data(set_floor_delta_function)
        resources.config_table.grant_read_write_data(set_floor_delta_function)
        set_floor_delta_schedule = events.Schedule.rate(cdk.Duration.minutes(10))
        set_floor_delta_target = events_targets.LambdaFunction(handler=set_floor_delta_function)
        events.Rule(
            scope,
            "SetFloorDeltaRule",
            description="Periodically sets floor delta",
            enabled=True,
            schedule=set_floor_delta_schedule,
            targets=[set_floor_delta_target]
        )
        # GET Median DELTA
        self.get_median_delta_function = _lambda.Function(
            scope,
            'GetMedianDelta',
            function_name='GetMedianDelta',
            timeout=cdk.Duration.seconds(30),
            handler='art.lists.get_median_delta.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(self.get_median_delta_function)
        # SET Median DELTA
        set_median_delta_function = _lambda.Function(
            scope,
            'SetMedianDelta',
            function_name='SetMedianDelta',
            handler='art.set_lists.median_delta.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('../src'),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        resources.collections_table.grant_read_data(set_median_delta_function)
        resources.config_table.grant_read_write_data(set_median_delta_function)
        set_median_delta_schedule = events.Schedule.rate(cdk.Duration.minutes(10))
        set_median_delta_target = events_targets.LambdaFunction(handler=set_median_delta_function)
        events.Rule(
            scope,
            "SetMedianDeltaRule",
            description="Periodically sets median delta",
            enabled=True,
            schedule=set_median_delta_schedule,
            targets=[set_median_delta_target]
        )
        # GET Volume DELTA
        self.get_volume_delta_function = _lambda.Function(
            scope,
            'GetVolumeDelta',
            function_name='GetVolumeDelta',
            timeout=cdk.Duration.seconds(30),
            handler='art.lists.get_volume_delta.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(self.get_volume_delta_function)
        # SET Volume DELTA
        set_volume_delta_function = _lambda.Function(
            scope,
            'SetVolumeDelta',
            function_name='SetVolumeDelta',
            handler='art.set_lists.volume_delta.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('../src'),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        resources.collections_table.grant_read_data(set_volume_delta_function)
        resources.config_table.grant_read_write_data(set_volume_delta_function)
        set_volume_delta_schedule = events.Schedule.rate(cdk.Duration.minutes(10))
        set_volume_delta_target = events_targets.LambdaFunction(handler=set_volume_delta_function)
        events.Rule(
            scope,
            "SetVolumeDeltaRule",
            description="Periodically sets volume delta",
            enabled=True,
            schedule=set_volume_delta_schedule,
            targets=[set_volume_delta_target]
        )
        # GET Charts
        self.get_charts_function = _lambda.Function(
            scope,
            'GetCharts',
            function_name='GetCharts',
            timeout=cdk.Duration.seconds(30),
            handler='art.lists.get_charts.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(self.get_charts_function)
        resources.collections_table.grant_read_write_data(self.get_charts_function)
        resources.grant_read_index_data(
            self.get_charts_function,
            [resources.art_table, resources.collections_table]
        )
        # SET Buyers DELTA
        set_buyers_delta_function = _lambda.Function(
            scope,
            'SetBuyersDelta',
            function_name='SetBuyersDelta',
            handler='art.set_lists.buyers_delta.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('../src'),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        resources.collections_table.grant_read_data(set_buyers_delta_function)
        resources.config_table.grant_read_write_data(set_buyers_delta_function)
        set_buyers_delta_schedule = events.Schedule.rate(cdk.Duration.minutes(10))
        set_buyers_delta_target = events_targets.LambdaFunction(handler=set_buyers_delta_function)
        events.Rule(
            scope,
            "SetBuyersDeltaRule",
            description="Periodically sets buyers delta",
            enabled=True,
            schedule=set_buyers_delta_schedule,
            targets=[set_buyers_delta_target]
        )
        # NEW LISTINGS
        new_listings_function = _lambda.Function(
            scope,
            'NewListings',
            function_name='NewListings',
            timeout=cdk.Duration.seconds(30),
            handler='art.events.new_listings.lambda_handler',
            **lambda_default_kwargs
        )

        new_listings_schedule = events.Schedule.rate(cdk.Duration.seconds(60))
        new_listings_target = events_targets.LambdaFunction(handler=new_listings_function)
        events.Rule(
            scope,
            "NewListingsRule",
            description="Periodically add and updates listings",
            enabled=True,
            schedule=new_listings_schedule,
            targets=[new_listings_target]
        )

        resources.listings_topic.grant_publish(new_listings_function)
        resources.config_table.grant_read_write_data(new_listings_function)
        # LISTINGS PROCESSOR
        listings_processor_function = _lambda.Function(
            scope,
            'ListingsProcessor',
            function_name='ListingsProcessor',
            timeout=cdk.Duration.seconds(30),
            handler='art.events.process_listings.lambda_handler',
            **lambda_default_kwargs
        )
        resources.listings_topic.add_subscription(
            subs.LambdaSubscription(
                listings_processor_function
            )
        )
        resources.art_table.grant_read_write_data(listings_processor_function)
        resources.grant_read_index_data(
            listings_processor_function,
            [resources.art_table]
        )
        resources.collections_table.grant_read_write_data(listings_processor_function)
        # GET OS STATS
        get_os_stats_function = _lambda.Function(
            scope,
            'GetosStats',
            function_name='GetosStats',
            timeout=cdk.Duration.seconds(300),
            handler='art.events.get_os_stats.lambda_handler',
            **lambda_default_kwargs
        )

        get_os_stats_schedule = events.Schedule.rate(cdk.Duration.minutes(5))
        get_os_stats_target = events_targets.LambdaFunction(handler=get_os_stats_function)
        events.Rule(
            scope,
            "GetosStatsRule",
            description="Periodically refreshes trending arts sorted by click counts",
            enabled=True,
            schedule=get_os_stats_schedule,
            targets=[get_os_stats_target]
        )
        resources.collections_table.grant_read_write_data(get_os_stats_function)
        resources.grant_read_index_data(
            get_os_stats_function,
            [resources.collections_table]
        )
        # GET ETH RATE
        self.get_ethrate_function = _lambda.Function(
            scope,
            'GetEthRate',
            function_name='GetEthRate',
            handler='art.getEthRate.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_data(self.get_ethrate_function)
        # SET MACRO_STATS
        macro_stats_function = _lambda.Function(
            scope,
            'MacroStats',
            function_name='MacroStats',
            timeout=cdk.Duration.seconds(30),
            handler='art.set_lists.macro_stats.lambda_handler',
            **lambda_default_kwargs
        )

        macro_stats_schedule = events.Schedule.rate(cdk.Duration.minutes(60))
        macro_stats_target = events_targets.LambdaFunction(handler=macro_stats_function)
        events.Rule(
            scope,
            "MacroStatsRule",
            description="Periodically sets Macro Stats",
            enabled=True,
            schedule=macro_stats_schedule,
            targets=[macro_stats_target]
        )
        resources.config_table.grant_read_write_data(macro_stats_function)
        # GET MACRO STATS
        self.get_macro_stats_function = _lambda.Function(
            scope,
            'GetMacroStats',
            function_name='GetMacroStats',
            timeout=cdk.Duration.seconds(30),
            handler='art.lists.get_macro_stats.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(self.get_macro_stats_function)
        # ADD SCORE
        self.add_score_function = _lambda.Function(
            scope,
            'AddScore',
            function_name='AddScore',
            handler='art.set_lists.add_score.lambda_handler',
            **lambda_default_kwargs
        )
        resources.add_score_topic.add_subscription(
            subs.LambdaSubscription(
                self.add_score_function
            )
        )
        resources.collections_table.grant_read_write_data(self.add_score_function)













