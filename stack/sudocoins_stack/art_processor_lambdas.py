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
        start_label_detection_topic = sns.Topic(
            scope,
            'ArtProcessorStartLabelDetectionTopic',
            display_name='ArtProcessorStartLabelDetection',
            topic_name='ArtProcessorStartLabelDetection'
        )
        art_processor_publish_role = iam.Role(
            scope,
            'RekognitionArtProcessorStartLabelDetectionTopicPublishRole',
            assumed_by=iam.ServicePrincipal('rekognition.amazonaws.com'),
            inline_policies={
                'ArtProcessorStartLabelDetectionTopicPublish': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            resources=[start_label_detection_topic.topic_arn],
                            actions=['sns:Publish']
                        )
                    ]
                )
            }
        )
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
        # REKOGNITION START
        rekognition_start_function = _lambda.Function(
            scope,
            'ArtProcessorRekognitionStart',
            function_name='ArtProcessorRekognitionStart',
            handler='art.artprocessor.rekognition_start.lambda_handler',
            timeout=cdk.Duration.seconds(60),
            **lambda_default_kwargs
        )
        rekognition_start_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['*'],
                actions=['rekognition:DetectLabels', 'rekognition:StartLabelDetection']
            )
        )
        resources.art_table.grant_read_write_data(rekognition_start_function)
        resources.art_processor_topic.grant_publish(rekognition_start_function)
        resources.art_processor_topic.add_subscription(
            subs.LambdaSubscription(
                rekognition_start_function,
                filter_policy={
                    'process': sns.SubscriptionFilter.string_filter(allowlist=['REKOGNITION_START'])
                }
            )
        )
        resources.art_bucket.grant_read_write(rekognition_start_function)
        # REKOGNITION END
        rekognition_end_function = _lambda.Function(
            scope,
            'ArtProcessorRekognitionEnd',
            function_name='ArtProcessorRekognitionEnd',
            handler='art.artprocessor.rekognition_end.lambda_handler',
            timeout=cdk.Duration.seconds(60),
            **lambda_default_kwargs
        )
        rekognition_end_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['*'],
                actions=['rekognition:GetLabelDetection']
            )
        )
        resources.art_table.grant_read_write_data(rekognition_end_function)
        resources.art_processor_topic.grant_publish(rekognition_end_function)
        start_label_detection_topic.add_subscription(
            subs.LambdaSubscription(
                rekognition_end_function
            )
        )
        resources.art_processor_topic.add_subscription(
            subs.LambdaSubscription(
                rekognition_end_function,
                filter_policy={
                    'process': sns.SubscriptionFilter.string_filter(allowlist=['REKOGNITION_END'])
                }
            )
        )
        resources.art_bucket.grant_read_write(rekognition_end_function)
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
