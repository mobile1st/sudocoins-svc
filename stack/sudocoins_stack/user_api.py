from resources import SudocoinsImportedResources
from user_lambdas import SudocoinsUserLambdas
from aws_cdk import (
    core as cdk,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations
)


class SudocoinsUserApi:
    default_cors_preflight = {
        'allow_methods': [apigwv2.CorsHttpMethod.ANY],
        'allow_origins': ['*'],
        'allow_headers': ['Content-Type', 'X-Amz-Date', 'Authorization', 'X-Api-Key', 'X-Amz-Security-Token',
                          'X-Amz-User-Agent']
    }

    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources,
                 lambdas: SudocoinsUserLambdas):
        user_api_v2 = apigwv2.HttpApi(
            scope,
            'UserApiV2',
            cors_preflight=self.default_cors_preflight
        )
        # GET PROFILE
        get_profile_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_profile_function
        )
        user_api_v2.add_routes(
            path='/user/profile',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_profile_integration
        )
        # UPDATE PROFILE
        update_profile_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.update_profile_function
        )
        user_api_v2.add_routes(
            path='/user/profile/update',
            methods=[apigwv2.HttpMethod.POST],
            integration=update_profile_integration
        )
        # USER VERIFY
        user_verify_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.user_verify_function
        )
        user_api_v2.add_routes(
            path='/user/verify',
            methods=[apigwv2.HttpMethod.POST],
            integration=user_verify_integration
        )
        # CASH OUT
        cash_out_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.cash_out_function
        )
        user_api_v2.add_routes(
            path='/user/cash-out',
            methods=[apigwv2.HttpMethod.POST],
            integration=cash_out_integration,
            authorizer=resources.sudocoins_authorizer
        )
