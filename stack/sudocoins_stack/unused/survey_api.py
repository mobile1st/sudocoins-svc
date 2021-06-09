from resources import SudocoinsImportedResources
from survey_lambdas import SudocoinsSurveyLambdas
from aws_cdk import (
    core as cdk,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations
)


class SudocoinsSurveyApi:
    default_cors_preflight = {
        'allow_methods': [apigwv2.CorsHttpMethod.ANY],
        'allow_origins': ['*'],
        'allow_headers': ['Content-Type', 'X-Amz-Date', 'Authorization', 'X-Api-Key', 'X-Amz-Security-Token',
                          'X-Amz-User-Agent']
    }

    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources,
                 lambdas: SudocoinsSurveyLambdas):
        survey_api_v2 = apigwv2.HttpApi(
            scope,
            'SurveyApiV2',
            cors_preflight=self.default_cors_preflight
        )
        # DYNATA CALLBACK
        dynata_callback_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.dynata_callback_function
        )
        survey_api_v2.add_routes(
            path='/survey/dynata/callback',
            methods=[apigwv2.HttpMethod.GET],
            integration=dynata_callback_integration
        )
        # DYNATA REDIRECT
        dynata_redirect_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.dynata_redirect_function
        )
        survey_api_v2.add_routes(
            path='/survey/dynata/redirect',
            methods=[apigwv2.HttpMethod.GET],
            integration=dynata_redirect_integration
        )
        # LUCID REDIRECT
        lucid_redirect_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.lucid_redirect_function
        )
        survey_api_v2.add_routes(
            path='/survey/lucid/redirect',
            methods=[apigwv2.HttpMethod.POST],
            integration=lucid_redirect_integration
        )
        # CINT SURVEY END
        cint_survey_end_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.survey_end_function
        )
        survey_api_v2.add_routes(
            path='/survey/cint/end',
            methods=[apigwv2.HttpMethod.GET],
            integration=cint_survey_end_integration
        )
        # TAKE SURVEY
        take_survey_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.take_survey_function
        )
        survey_api_v2.add_routes(
            path='/survey/start',
            methods=[apigwv2.HttpMethod.GET],
            integration=take_survey_integration
        )
