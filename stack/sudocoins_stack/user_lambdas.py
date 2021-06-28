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
            handler='art.get_profile.lambda_handler',
            description='Gets all data for displaying the profil page',
            **lambda_default_kwargs
        )
        resources.transaction_topic.grant_publish(self.get_profile_function)
        resources.profile_table.grant_read_write_data(self.get_profile_function)
        resources.sub_table.grant_read_write_data(self.get_profile_function)
        resources.config_table.grant_read_data(self.get_profile_function)
        resources.grant_read_index_data(self.get_profile_function, [resources.profile_table])
        # UPDATE PROFILE
        self.update_profile_function = _lambda.Function(
            scope,
            'UserUpdateProfileV2',
            function_name='UserUpdateProfileV2',
            handler='art.update_profile.lambda_handler',
            description='Updates profile related attributes',
            **lambda_default_kwargs
        )
        resources.profile_table.grant_read_write_data(self.update_profile_function)
        resources.sub_table.grant_read_data(self.update_profile_function)
        self.update_profile_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/Profile/index/*'],
                actions=['dynamodb:Query']
            )
        )
        # USER VERIFY
        self.user_verify_function = _lambda.Function(
            scope,
            'UserVerifyV2',
            function_name='UserVerifyV2',
            handler='art.user_verify.lambda_handler',
            description='Verifies user with google recaptcha',
            **lambda_default_kwargs
        )
        resources.profile_table.grant_read_write_data(self.user_verify_function)
        resources.verifications_table.grant_read_write_data(self.user_verify_function)
        # CASH OUT
        self.cash_out_function = _lambda.Function(
            scope,
            'UserCashOutV2',
            function_name='UserCashOutV2',
            handler='art.cash_out.lambda_handler',
            **lambda_default_kwargs
        )
        resources.profile_table.grant_read_write_data(self.cash_out_function)
        resources.payouts_table.grant_read_write_data(self.cash_out_function)
        resources.ledger_table.grant_read_write_data(self.cash_out_function)
        resources.transaction_table.grant_read_write_data(self.cash_out_function)
        resources.sub_table.grant_read_write_data(self.cash_out_function)
        resources.grant_read_index_data(self.cash_out_function, [resources.transaction_table])
        resources.grant_read_index_data(self.cash_out_function, [resources.ledger_table])
        self.cash_out_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['*'],
                actions=['sns:Publish']
            )
        )
        resources.grant_read_index_data(self.cash_out_function, [resources.transaction_table, resources.ledger_table])
        # MORE HISTORY
        self.more_history_function = _lambda.Function(
            scope,
            'MoreHistoryV2',
            function_name='MoreHistoryV2',
            handler='art.moreHistory.lambda_handler',
            description='loads more history for the user',
            **lambda_default_kwargs
        )
        resources.profile_table.grant_read_write_data(self.more_history_function)
        resources.sub_table.grant_read_write_data(self.more_history_function)
        resources.payouts_table.grant_read_write_data(self.more_history_function)
        resources.ledger_table.grant_read_write_data(self.more_history_function)
        resources.transaction_table.grant_read_write_data(self.more_history_function)
        resources.grant_read_index_data(self.more_history_function, [resources.transaction_table])
        resources.grant_read_index_data(self.more_history_function, [resources.ledger_table])
