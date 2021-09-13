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


class SudocoinsArtProcessorLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        # STREAM TO S3
        stream_to_s3_function = _lambda.Function(
            scope,
            'ArtProcessorStreamToS3',
            function_name='ArtProcessorStreamToS3',
            handler='art.artprocessor.stream_to_s3.lambda_handler',
            timeout=cdk.Duration.seconds(60),
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_write_data(stream_to_s3_function)
        resources.art_processor_topic.grant_publish(stream_to_s3_function)
        resources.art_processor_topic.add_subscription(
            subs.LambdaSubscription(
                stream_to_s3_function,
                filter_policy={
                    'process': sns.SubscriptionFilter.string_filter(allowlist=['STREAM_TO_S3'])
                }
            )
        )
        resources.art_bucket.grant_read_write(stream_to_s3_function)
        # RETRY ART PROCESSING
        processor_retry_function = _lambda.Function(
            scope,
            'ArtProcessorRetryV2',
            function_name='ArtProcessorRetryV2',
            handler='art.artprocessor.processor_retry.lambda_handler',
            **lambda_default_kwargs
        )
        resources.art_table.grant_read_data(processor_retry_function)
        resources.grant_read_index_data(processor_retry_function, [resources.art_table])
        set_retry_schedule = events.Schedule.rate(cdk.Duration.minutes(600))
        set_retry_target = events_targets.LambdaFunction(handler=processor_retry_function)
        events.Rule(
            scope,
            "ArtProcessorRetry",
            description="Periodically checks to see what Arts need to be re-processed",
            enabled=True,
            schedule=set_retry_schedule,
            targets=[set_retry_target]
        )
        resources.art_processor_topic.grant_publish(processor_retry_function)

