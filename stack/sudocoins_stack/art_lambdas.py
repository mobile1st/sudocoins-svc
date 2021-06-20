from resources import SudocoinsImportedResources
from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_lambda_python as lambda_python,
    aws_lambda_event_sources as event_sources,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_logs as logs
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
        resources.grant_read_index_data(set_leaderboard_function, [resources.profile_table])
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
            'AddVote',
            function_name='AddVote',
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
            'GetPreviewV2',
            function_name='GetPreviewV2',
            handler='art.get_preview.lambda_handler',
            timeout=cdk.Duration.seconds(5),
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.get_preview_function)
        resources.art_uploads_table.grant_read_write_data(self.get_preview_function)
        resources.grant_read_index_data(self.get_preview_function, [resources.art_table, resources.art_uploads_table])
