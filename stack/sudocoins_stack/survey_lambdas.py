from resources import SudocoinsImportedResources
from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_lambda_python as lambda_python,
    aws_lambda_event_sources as event_sources,
    aws_iam as iam
)

lambda_code_path = '../src'
lambda_runtime = _lambda.Runtime.PYTHON_3_8


class SudocoinsSurveyLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        # DYNATA CALLBACK
        self.dynata_callback_function = _lambda.Function(
            scope,
            'SurveyDynataCallbackV2',
            function_name='SurveyDynataCallbackV2',
            runtime=lambda_runtime,
            handler='survey.dynata_callback.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path),
            environment={'keyId': 'b7c2bdd3d1ed9259032a33f2d8c151dd'}
        )
        resources.end_transaction_queue.grant_send_messages(self.dynata_callback_function)
        resources.config_table.grant_read_data(self.dynata_callback_function)
        # DYNATA REDIRECT
        self.dynata_redirect_function = _lambda.Function(
            scope,
            'SurveyDynataRedirectV2',
            function_name='SurveyDynataRedirectV2',
            runtime=lambda_runtime,
            handler='survey.dynata_redirect.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path),
            environment={'CONFIG_TABLE': 'Config'}
        )
        resources.end_transaction_queue.grant_send_messages(self.dynata_redirect_function)
        resources.transaction_table.grant_read_data(self.dynata_redirect_function)
        resources.config_table.grant_read_data(self.dynata_redirect_function)
        # LUCID REDIRECT
        self.lucid_redirect_function = _lambda.Function(
            scope,
            'SurveyLucidRedirectV2',
            function_name='SurveyLucidRedirectV2',
            runtime=lambda_runtime,
            handler='survey.lucid_redirect.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path),
            environment={'key': 'wQA9elIexoOpPy4Wn9QqdBGlAZy6TJRfofcqrKuMX7ZCvRoJO2XRtr1SenMVXsZbx1rMCJnnGaP2S1zwUkcbB'}
        )
        resources.end_transaction_queue.grant_send_messages(self.lucid_redirect_function)
        # SURVEY END
        self.survey_end_function = _lambda.Function(
            scope,
            'SurveyEndV2',
            function_name='SurveyEndV2',
            runtime=lambda_runtime,
            handler='survey.survey_end.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path),
            environment={'keyId': '18d5a58740d001789f8f41a830d7ae33'}
        )
        resources.end_transaction_queue.grant_send_messages(self.survey_end_function)
        resources.config_table.grant_read_data(self.survey_end_function)
        # TAKE SURVEY
        self.take_survey_function = lambda_python.PythonFunction(
            scope,
            'SurveyStartV2',
            function_name='SurveyStartV2',
            entry=lambda_code_path,
            index='survey/take_survey.py',
            handler='lambda_handler',
            runtime=lambda_runtime
        )
        resources.transaction_topic.grant_publish(self.take_survey_function)
        resources.config_table.grant_read_data(self.take_survey_function)
        resources.profile_table.grant_read_write_data(self.take_survey_function)
        resources.sub_table.grant_read_data(self.take_survey_function)
        resources.transaction_table.grant_read_write_data(self.take_survey_function)
        # END TRANSACTION
        end_transaction_function = _lambda.Function(
            scope,
            'SurveyEndTransactionV2',
            function_name='SurveyEndTransactionV2',
            runtime=lambda_runtime,
            handler='survey.end_transaction.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.transaction_topic.grant_publish(end_transaction_function)
        resources.transaction_table.grant_read_write_data(end_transaction_function)
        resources.config_table.grant_read_data(end_transaction_function)
        resources.profile_table.grant_read_write_data(end_transaction_function)
        resources.ledger_table.grant_read_write_data(end_transaction_function)
        event_source = end_transaction_function.add_event_source(
            event_sources.SqsEventSource(
                resources.end_transaction_queue,
                batch_size=10,
                enabled=False
            )
        )
