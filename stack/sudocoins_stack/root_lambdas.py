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
    aws_iam as iam,
    aws_lambda_python as lambda_python
)

lambda_default_kwargs = {
    'runtime': _lambda.Runtime.PYTHON_3_8,
    'code': _lambda.Code.asset('../src'),
    'memory_size': 512,
    'log_retention': logs.RetentionDays.THREE_MONTHS
}


class SudocoinsRootLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        self.social_share_function = _lambda.Function(
            scope,
            'RootSocialShareV1',
            function_name='RootSocialShareV1',
            handler='art.get_preview.lambda_handler',
            timeout=cdk.Duration.seconds(5),
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(self.social_share_function)
        resources.art_uploads_table.grant_read_write_data(self.social_share_function)
        resources.grant_read_index_data(self.social_share_function, [resources.art_table, resources.art_uploads_table])
        # TEST
        lambda_python.PythonFunction(
            scope,
            'TestDependency',
            entry='../src',
            index='test_dependency.py',
            handler='lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_8
        )
