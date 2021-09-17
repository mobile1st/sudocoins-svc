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
        resources.art_counter_queue.grant_send_messages(self.increment_view_count_function)
        # GET SHARED ART
        self.get_shared_art_function = _lambda.Function(
            scope,
            'ArtGetSharedArtV2',
            function_name='ArtGetSharedArtV2',
            handler='art.get_shared_art.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_data(self.get_shared_art_function)
        resources.art_votes_table.grant_read_data(self.get_shared_art_function)
        resources.art_uploads_table.grant_read_data(self.get_shared_art_function)
        resources.art_counter_queue.grant_send_messages(self.get_shared_art_function)
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
        resources.art_votes_table.grant_read_write_data(self.art_source_redirect_function)
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
            handler='art.get_recent.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_data(self.get_recent_function)
        resources.grant_read_index_data(self.get_recent_function, [resources.art_table])
        # GET TRENDING
        self.get_trending_function = _lambda.Function(
            scope,
            'ArtGetTrendingV2',
            function_name='ArtGetTrendingV2',
            handler='art.get_trending.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_data(self.get_trending_function)
        # SET TRENDING
        set_trending_function = _lambda.Function(
            scope,
            'ArtSetTrendingV2',
            function_name='ArtSetTrendingV2',
            handler='art.set_trending.lambda_handler',
            timeout=cdk.Duration.seconds(300),
            memory_size=1024,
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('../src'),
            log_retention=logs.RetentionDays.THREE_MONTHS
        )
        resources.art_table.grant_read_data(set_trending_function)
        resources.config_table.grant_read_write_data(set_trending_function)
        resources.grant_read_index_data(set_trending_function, [resources.art_table])
        resources.grant_read_index_data(set_trending_function, [resources.art_votes_table])
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
        # GET LEADERBOARD
        self.get_leaderboard_function = _lambda.Function(
            scope,
            'ArtGetLeaderboardV2',
            function_name='ArtGetLeaderboardV2',
            handler='art.get_leaderboard.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_data(self.get_leaderboard_function)
        # GET USER ARTS
        self.get_user_arts_function = _lambda.Function(
            scope,
            'ArtGetUserArtsV2',
            function_name='ArtGetUserArtsV2',
            handler='art.get_user_arts.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_uploads_table.grant_read_data(self.get_user_arts_function)
        resources.grant_read_index_data(self.get_user_arts_function, [resources.art_table, resources.art_uploads_table])
        resources.art_table.grant_read_data(self.get_user_arts_function)
        # SHARE ART
        self.share_art_function = _lambda.Function(
            scope,
            'ArtShareV2',
            function_name='ArtShareV2',
            handler='art.share_art.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_data(self.share_art_function)
        resources.art_uploads_table.grant_read_write_data(self.share_art_function)
        resources.profile_table.grant_read_write_data(self.share_art_function)
        resources.grant_read_index_data(self.share_art_function, [resources.art_uploads_table])
        # REGISTER CLICK
        register_click_function = _lambda.Function(
            scope,
            'ArtRegisterClickV2',
            function_name='ArtRegisterClickV2',
            handler='art.register_click.lambda_handler',
            timeout=cdk.Duration.seconds(15),
            **lambda_default_kwargs
        )
        resources.profile_table.grant_read_write_data(register_click_function)
        resources.ledger_table.grant_read_write_data(register_click_function)
        resources.art_table.grant_read_write_data(register_click_function)
        resources.art_uploads_table.grant_read_write_data(register_click_function)
        resources.art_votes_table.grant_read_write_data(register_click_function)
        register_click_function.add_event_source(
            event_sources.SqsEventSource(
                resources.art_counter_queue,
                batch_size=10,
                enabled=True
            )
        )
        resources.grant_read_index_data(register_click_function, [resources.ledger_table])
        # ADD VOTE
        self.add_vote_function = _lambda.Function(
            scope,
            'ArtAddVote',
            function_name='ArtAddVote',
            handler='art.add_vote.lambda_handler',
            timeout=cdk.Duration.seconds(5),
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.add_vote_function)
        resources.art_votes_table.grant_read_write_data(self.add_vote_function)
        resources.grant_read_index_data(self.add_vote_function, [resources.art_table, resources.art_votes_table])
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
        # GET SHORT URL
        self.get_short_url_function = _lambda.Function(
            scope,
            'ArtGetShortUrlV1',
            function_name='ArtGetShortUrlV1',
            handler='art.get_short_url.lambda_handler',
            timeout=cdk.Duration.seconds(5),
            **lambda_default_kwargs
        )
        # ART SEARCH
        self.art_search_function = _lambda.Function(
            scope,
            'ArtSearch',
            function_name='ArtSearch',
            handler='search.art_search.lambda_handler',
            **lambda_default_kwargs
        )
        resources.search_table.grant_read_write_data(self.art_search_function)
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
        # ARTIST PAGE
        self.artist_page_function = _lambda.Function(
            scope,
            'ArtistPageV2',
            function_name='ArtistPageV2',
            handler='art.artist_page.lambda_handler',
            timeout=cdk.Duration.seconds(15),
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.artist_page_function)
        resources.grant_read_index_data(
            self.artist_page_function,
            [resources.art_table]
        )
        # INGEST OPENSEA
        ingest_opensea_function = _lambda.Function(
            scope,
            'IngestOpenSeaV2',
            function_name='IngestOpenSeaV2',
            timeout=cdk.Duration.seconds(20),
            handler='art.ingest_opensea.lambda_handler',
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
        resources.grant_read_index_data(
            ingest_opensea_function,
            [resources.art_table]
        )
        # INGEST PROCESSOR
        ingest_processor_function = _lambda.Function(
            scope,
            'IngestProcessorV2',
            function_name='IngestProcessorV2',
            handler='art.ingest_processor.lambda_handler',
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
        resources.creators_table.grant_read_write_data(ingest_processor_function)
        # GET HEARTS
        self.get_hearts_function = _lambda.Function(
            scope,
            'GetHeartsV2',
            function_name='GetHeartsV2',
            handler='art.get_hearts.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_votes_table.grant_read_write_data(self.get_hearts_function)
        resources.grant_read_index_data(self.get_hearts_function, [resources.art_votes_table])
        # AUTO TWEET
        self.auto_tweet_function = _lambda.Function(
            scope,
            'AutoTweetV2',
            function_name='AutoTweetV2',
            handler='art.auto_tweet.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(self.auto_tweet_function)
        resources.art_table.grant_read_write_data(self.auto_tweet_function)
        resources.auto_tweet_table.grant_read_write_data(self.auto_tweet_function)
        '''
        auto_tweet_schedule = events.Schedule.rate(cdk.Duration.minutes(180))
        auto_tweet_target = events_targets.LambdaFunction(handler=self.auto_tweet_function)
        events.Rule(
            scope,
            "AutoTweetRule",
            description="Periodically tweets trending arts",
            enabled=True,
            schedule=auto_tweet_schedule,
            targets=[auto_tweet_target]
        )
        '''
        # AUTO FB
        self.auto_fb_function = _lambda.Function(
            scope,
            'AutoFBV2',
            function_name='AutoFBV2',
            handler='art.auto_fb.lambda_handler',
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
        # COLLECTION NAME
        self.collection_name_function = _lambda.Function(
            scope,
            'CollectionNameV2',
            function_name='CollectionNameV2',
            handler='art.get_collection_name.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.collection_name_function)
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
        resources.grant_read_index_data(
            self.delete_art_function,
            [resources.art_table]
        )




