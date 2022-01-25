from resources import SudocoinsImportedResources
from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_sns_subscriptions as subs,
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


class SudocoinsAdminLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):

        # SET RATES
        set_rates_function = _lambda.Function(
            scope,
            'AdminSetRatesV2',
            function_name='AdminSetRatesV2',
            handler='admin.set_rates.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(set_rates_function)
        set_rates_schedule = events.Schedule.rate(cdk.Duration.minutes(5))
        set_rates_target = events_targets.LambdaFunction(handler=set_rates_function)
        events.Rule(
            scope,
            'SetRatesRule',
            description='Call the BTC exchange rate function every 5 minutes',
            enabled=True,
            schedule=set_rates_schedule,
            targets=[set_rates_target]
        )