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
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_data(set_trending_function)
        resources.config_table.grant_read_write_data(set_trending_function)
        resources.grant_read_index_data(set_trending_function, [resources.art_table])
        resources.grant_read_index_data(set_trending_function, [resources.art_votes_table])
        set_trending_schedule = events.Schedule.rate(cdk.Duration.minutes(5))
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
        # SET LEADERBOARD
        set_leaderboard_function = _lambda.Function(
            scope,
            'ArtSetLeaderboardV2',
            function_name='ArtSetLeaderboardV2',
            handler='art.set_leaderboard.lambda_handler',
            **lambda_default_kwargs
        )
        resources.profile_table.grant_read_data(set_leaderboard_function)
        resources.config_table.grant_read_write_data(set_leaderboard_function)
        resources.art_votes_table.grant_read_data(set_leaderboard_function)
        resources.art_table.grant_read_data(set_leaderboard_function)
        resources.grant_read_index_data(set_leaderboard_function, [resources.profile_table, resources.art_votes_table])
        set_leaderboard_schedule = events.Schedule.rate(cdk.Duration.minutes(5))
        set_leaderboard_target = events_targets.LambdaFunction(handler=set_leaderboard_function)
        events.Rule(
            scope,
            "SetLeaderboardRule",
            description="Periodically refreshes the promoter leaderboard",
            enabled=True,
            schedule=set_leaderboard_schedule,
            targets=[set_leaderboard_target]
        )
        # GET USER ARTS
        self.get_user_arts_function = _lambda.Function(
            scope,
            'ArtGetUserArtsV2',
            function_name='ArtGetUserArtsV2',
            handler='art.get_user_arts.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_uploads_table.grant_read_data(self.get_user_arts_function)
        resources.grant_read_index_data(self.get_user_arts_function, [resources.art_uploads_table])
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
        # SET ARTISTS
        set_artists_function = _lambda.Function(
            scope,
            'ArtSetArtistsV2',
            function_name='ArtSetArtistsV2',
            handler='art.set_artist_leaderboard.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_data(set_artists_function)
        resources.config_table.grant_read_write_data(set_artists_function)
        resources.grant_read_index_data(set_artists_function, [resources.art_votes_table])
        set_artists_schedule = events.Schedule.rate(cdk.Duration.minutes(5))
        set_artists_target = events_targets.LambdaFunction(handler=set_artists_function)
        events.Rule(
            scope,
            "SetArtistsRule",
            description="Periodically refreshes logic to determine the top artists based on 1 day of data",
            enabled=True,
            schedule=set_artists_schedule,
            targets=[set_artists_target]
        )
        # ART SEARCH
        self.art_search_function = _lambda.Function(
            scope,
            'ArtSearch',
            function_name='ArtSearch',
            handler='art.art_search.lambda_handler',
            **lambda_default_kwargs
        )
        self.art_search_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    'arn:aws:kendra:us-west-2:977566059069:index/8f96a3bb-3aae-476e-94ec-0d446877b42a',
                    'arn:aws:kendra:us-west-2:977566059069:index/8f96a3bb-3aae-476e-94ec-0d446877b42a/data-source/52596114-645e-40fa-b154-3ada7b3a7942'
                ],
                actions=['kendra:Query']
            )
        )
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
            timeout=cdk.Duration.seconds(5),
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
            handler='art.ingest_opensea.lambda_handler',
            **lambda_default_kwargs
        )
        ingest_opensea_schedule = events.Schedule.rate(cdk.Duration.minutes(20))
        ingest_opensea_target = events_targets.LambdaFunction(handler=ingest_opensea_function)
        events.Rule(
            scope,
            "IngestOpenseaRule",
            description="Periodically refreshes trending arts sorted by click counts",
            enabled=True,
            schedule=ingest_opensea_schedule,
            targets=[ingest_opensea_target]
        )
        resources.ingest_opensea_topic.grant_publish(self.ingest_opensea_function)
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
        resources.art_table.grant_read_write_data(self.ingest_processor_function)
        resources.grant_read_index_data(
            self.ingest_processor_function,
            [resources.art_table]
        )







