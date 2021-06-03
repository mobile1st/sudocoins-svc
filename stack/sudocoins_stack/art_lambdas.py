from resources import SudocoinsImportedResources
from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_lambda_python as lambda_python,
    aws_lambda_event_sources as event_sources,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_logs as logs
)

lambda_code_path = '../src'
lambda_runtime = _lambda.Runtime.PYTHON_3_8
lambda_default_kwargs = {
    'runtime': _lambda.Runtime.PYTHON_3_8,
    'code': _lambda.Code.asset('../src'),
    'log_retention': logs.RetentionDays.THREE_MONTHS
}


class SudocoinsArtLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        # GET PROFILE
        self.get_profile_function = _lambda.Function(
            scope,
            'ArtGetProfileV2',
            function_name='ArtGetProfileV2',
            handler='art.get_profile.lambda_handler',
            memory_size=512,
            description='Gets all data for displaying the profil page',
            **lambda_default_kwargs
        )
        resources.profile_table.grant_read_write_data(self.get_profile_function)
        resources.sub_table.grant_read_write_data(self.get_profile_function)
        resources.config_table.grant_read_data(self.get_profile_function)
        self.get_profile_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/Profile/index/*'],
                actions=['dynamodb:Query']
            )
        )
        # ADD ART
        self.add_art_function = _lambda.Function(
            scope,
            'ArtAddV2',
            function_name='ArtAddV2',
            handler='art.add_art.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.add_art_function)
        resources.art_uploads_table.grant_read_write_data(self.add_art_function)
        resources.profile_table.grant_read_write_data(self.add_art_function)
        resources.ledger_table.grant_read_write_data(self.add_art_function)
        self.add_art_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/art/index/*'],
                actions=['dynamodb:Query']
            )
        )
        self.add_art_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/Ledger/index/*'],
                actions=['dynamodb:Query']
            )
        )
        self.add_art_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/art_uploads/index/*'],
                actions=['dynamodb:Query']
            )
        )
        # INCREMENT VIEW COUNT
        self.increment_view_count_function = _lambda.Function(
            scope,
            'ArtIncrementViewCountV2',
            function_name='ArtIncrementViewCountV2',
            runtime=lambda_runtime,
            handler='art.increment_view_count.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_counter_queue.grant_send_messages(self.increment_view_count_function)
        # GET SHARED ART
        self.get_shared_art_function = _lambda.Function(
            scope,
            'ArtGetSharedArtV2',
            function_name='ArtGetSharedArtV2',
            runtime=lambda_runtime,
            handler='art.get_shared_art.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_table.grant_read_data(self.get_shared_art_function)
        resources.art_uploads_table.grant_read_data(self.get_shared_art_function)
        resources.art_counter_queue.grant_send_messages(self.get_shared_art_function)
        # ART SOURCE REDIRECT
        self.art_source_redirect_function = _lambda.Function(
            scope,
            'ArtSourceRedirectV2',
            function_name='ArtSourceRedirectV2',
            runtime=lambda_runtime,
            handler='art.art_source_redirect.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_table.grant_read_write_data(self.art_source_redirect_function)
        resources.art_uploads_table.grant_read_write_data(self.art_source_redirect_function)
        # GET ARTS
        self.get_arts_function = _lambda.Function(
            scope,
            'ArtBatchGetV2',
            function_name='ArtBatchGetV2',
            runtime=lambda_runtime,
            handler='art.get_arts.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_table.grant_read_data(self.get_arts_function)
        # GET RECENT
        self.get_recent_function = _lambda.Function(
            scope,
            'ArtGetRecentV2',
            function_name='ArtGetRecentV2',
            runtime=lambda_runtime,
            handler='art.getRecent.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_table.grant_read_data(self.get_recent_function)
        self.get_recent_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/art/index/*'],
                actions=['dynamodb:Query']
            )
        )
        # GET TRENDING
        self.get_trending_function = _lambda.Function(
            scope,
            'ArtGetTrendingV2',
            function_name='ArtGetTrendingV2',
            runtime=lambda_runtime,
            handler='art.get_trending.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.config_table.grant_read_data(self.get_trending_function)
        # SET TRENDING
        set_trending_function = _lambda.Function(
            scope,
            'ArtSetTrendingV2',
            function_name='ArtSetTrendingV2',
            runtime=lambda_runtime,
            handler='art.set_trending.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_table.grant_read_data(set_trending_function)
        resources.config_table.grant_read_write_data(set_trending_function)
        set_trending_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/art/index/*'],
                actions=['dynamodb:Query']
            )
        )
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
            runtime=lambda_runtime,
            handler='art.get_leaderboard.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.config_table.grant_read_data(self.get_leaderboard_function)
        # SET LEADERBOARD
        set_leaderboard_function = _lambda.Function(
            scope,
            'ArtSetLeaderboardV2',
            function_name='ArtSetLeaderboardV2',
            runtime=lambda_runtime,
            handler='art.set_leaderboard.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.profile_table.grant_read_data(set_leaderboard_function)
        resources.config_table.grant_read_write_data(set_leaderboard_function)
        set_leaderboard_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/Profile/index/*'],
                actions=['dynamodb:Query']
            )
        )
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
        # MY GALLERY
        self.my_gallery_function = _lambda.Function(
            scope,
            'ArtMyGalleryV2',
            function_name='ArtMyGalleryV2',
            runtime=lambda_runtime,
            handler='art.myGallery.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_uploads_table.grant_read_data(self.my_gallery_function)
        self.my_gallery_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/art_uploads/index/*'],
                actions=['dynamodb:Query']
            )
        )
        # SHARE ART
        self.share_art_function = _lambda.Function(
            scope,
            'ArtShareV2',
            function_name='ArtShareV2',
            runtime=lambda_runtime,
            handler='art.shareArt.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_table.grant_read_data(self.share_art_function)
        resources.art_uploads_table.grant_read_write_data(self.share_art_function)
        resources.profile_table.grant_read_write_data(self.share_art_function)
        self.share_art_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/art_uploads/index/*'],
                actions=['dynamodb:Query']
            )
        )
        # REGISTER CLICK
        register_click_function = _lambda.Function(
            scope,
            'ArtRegisterClickV2',
            function_name='ArtRegisterClickV2',
            runtime=lambda_runtime,
            handler='art.register_click.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
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
        register_click_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/Ledger/index/*'],
                actions=['dynamodb:Query']
            )
        )