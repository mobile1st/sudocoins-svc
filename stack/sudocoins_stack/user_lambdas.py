from resources import SudocoinsImportedResources
from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_lambda_event_sources as event_sources,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_logs as logs,
    aws_sns_subscriptions as subs
)

lambda_default_kwargs = {
    'runtime': _lambda.Runtime.PYTHON_3_8,
    'code': _lambda.Code.asset('../src'),
    'memory_size': 512,
    'log_retention': logs.RetentionDays.THREE_MONTHS
}

class SudocoinsUserLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        # GET PROFILE
        self.get_profile_function = _lambda.Function(
            scope,
            'UserGetProfileV2',
            function_name='UserGetProfileV2',
            handler='user.get_profile.lambda_handler',
            description='Gets all data for displaying the profil page',
            **lambda_default_kwargs
        )
        resources.sub_table.grant_read_write_data(self.get_profile_function)
        resources.config_table.grant_read_data(self.get_profile_function)

        # GET PROFILE DEV
        self.get_profile_dev_function = _lambda.Function(
            scope,
            'UserGetProfileDEV',
            function_name='UserGetProfileDEV',
            handler='user.get_profile_dev.lambda_handler',
            timeout=cdk.Duration.seconds(60),
            description='Gets all data for displaying the profil page',
            **lambda_default_kwargs
        )
        resources.sub_table.grant_read_write_data(self.get_profile_dev_function)
        resources.config_table.grant_read_data(self.get_profile_dev_function)

        # GET USERID FOR META USER
        self.get_user_id_function = _lambda.Function(
            scope,
            'UserGetMetaIdV2',
            function_name='UserGetMetaIdV2',
            handler='user.get_user_id.lambda_handler',
            description='Get userId for meta mask user by publicAddress',
            **lambda_default_kwargs
        )
        resources.sub_table.grant_read_write_data(self.get_user_id_function)

        # USER VERIFY
        self.user_verify_function = _lambda.Function(
            scope,
            'UserVerifyV2',
            function_name='UserVerifyV2',
            handler='user.user_verify.lambda_handler',
            description='Verifies user with google recaptcha',
            **lambda_default_kwargs
        )
        resources.verifications_table.grant_read_write_data(self.user_verify_function)

        # CONTACT US
        self.contact_function = _lambda.Function(
            scope,
            'UserContactUsV2',
            function_name='UserContactUsV2',
            handler='user.contact.lambda_handler',
            **lambda_default_kwargs
        )
        resources.contact_table.grant_read_write_data(self.contact_function)
        self.contact_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['*'],
                actions=['sns:Publish']
            )
        )

        # TWITTER AUTH TOKEN
        self.get_twitter_token_function = _lambda.Function(
            scope,
            'RequestTwToken',
            function_name='RequestTwToken',
            handler='user.tw_request_token.lambda_handler',
            description='Returning twitter auth token',
            **lambda_default_kwargs
        )
        # SET PORTFOLIO
        self.set_portfolio_function = _lambda.Function(
            scope,
            'SetPortfolio',
            function_name='SetPortfolio',
            handler='user.set_portfolio.lambda_handler',
            description='Sets the user portfolio',
            **lambda_default_kwargs
        )
        resources.sub_table.grant_read_write_data(self.set_portfolio_function)
        resources.portfolio_table.grant_read_write_data(self.set_portfolio_function)

        # GET PORTFOLIO
        self.get_portfolio_function = _lambda.Function(
            scope,
            'GetPortfolio',
            function_name='GetPortfolio',
            handler='user.get_portfolio.lambda_handler',
            description='Gets the user portfolio',
            **lambda_default_kwargs
        )
        resources.sub_table.grant_read_write_data(self.get_portfolio_function)
        resources.portfolio_table.grant_read_write_data(self.get_portfolio_function)
        resources.collections_table.grant_read_write_data(self.get_portfolio_function)
        resources.grant_read_index_data(self.get_portfolio_function, [resources.portfolio_table])
        # IMPORT NFTS
        self.import_nfts_function = _lambda.Function(
            scope,
            'ImportNFTs',
            function_name='ImportNFTs',
            handler='user.import_nfts.lambda_handler',
            description='Gets the user portfolio from Meta Mask',
            **lambda_default_kwargs
        )
        resources.sub_table.grant_read_write_data(self.import_nfts_function)
        resources.portfolio_table.grant_read_write_data(self.import_nfts_function)
        resources.collections_table.grant_read_write_data(self.import_nfts_function)
        resources.grant_read_index_data(self.import_nfts_function, [resources.portfolio_table])

