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

class SudocoinsEtherLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        # Ether Smart Contract Events Scan CronJob
        ether_events_scan_cronjob = _lambda.Function(
            scope,
            'EtherEventScanJob',
            function_name='EtherEventScanJob',
            handler='ether.ether_event_scan_job.lambda_handler',
            timeout=cdk.Duration.seconds(298),
            **lambda_default_kwargs
        )
        resources.ether_events_table.grant_read_write_data(ether_events_scan_cronjob)
        set_event_scan_schedule = events.Schedule.rate(cdk.Duration.minutes(5))
        set_event_scan_target = events_targets.LambdaFunction(handler=ether_events_scan_cronjob)
        events.Rule(
            scope,
            "SetEtherEventScanJobRule",
            description="Scanning Ether Smart Contract events for each minute",
            enabled=True,
            schedule=set_event_scan_schedule,
            targets=[set_event_scan_target]
        )