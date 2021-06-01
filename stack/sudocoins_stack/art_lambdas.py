from resources import SudocoinsImportedResources
from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_lambda_python as lambda_python,
    aws_lambda_event_sources as event_sources,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam
)

lambda_code_path = '../src'
lambda_runtime = _lambda.Runtime.PYTHON_3_8


class SudocoinsArtLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        # ADD ART
        self.add_art_function = _lambda.Function(
            scope,
            'ArtAddV2',
            function_name='ArtAddV2',
            runtime=lambda_runtime,
            handler='art.addArt.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
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
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/art_uploads/index/*'],
                actions=['dynamodb:Query']
            )
        )
        # ADD VIEW
        self.add_view_function = _lambda.Function(
            scope,
            'ArtAddViewV2',
            function_name='ArtAddViewV2',
            runtime=lambda_runtime,
            handler='art.add_view.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_counter_queue.grant_send_messages(self.add_view_function)
        # ART PROMPT
        self.art_prompt_function = _lambda.Function(
            scope,
            'ArtPromptV2',
            function_name='ArtPromptV2',
            runtime=lambda_runtime,
            handler='art.artPrompt.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_table.grant_read_data(self.art_prompt_function)
        resources.art_uploads_table.grant_read_data(self.art_prompt_function)
        resources.art_counter_queue.grant_send_messages(self.art_prompt_function)
        # ART REDIRECT
        self.art_redirect_function = _lambda.Function(
            scope,
            'ArtRedirectV2',
            function_name='ArtRedirectV2',
            runtime=lambda_runtime,
            handler='art.artRedirect.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.art_table.grant_read_write_data(self.art_redirect_function)
        resources.art_uploads_table.grant_read_write_data(self.art_redirect_function)
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
        resources.config_table.grant_read_data(set_trending_function)
        set_trending_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/art/index/*'],
                actions=['dynamodb:Query']
            )
        )
        lambda_schedule = events.Schedule.rate(cdk.Duration.minutes(5))
        event_lambda_target = events_targets.LambdaFunction(handler=set_trending_function)
        lambda_cw_event = events.Rule(
            scope,
            "SetTrendingRule",
            description="Periodically refreshes trending arts sorted by click counts",
            enabled=True,
            schedule=lambda_schedule,
            targets=[event_lambda_target]
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