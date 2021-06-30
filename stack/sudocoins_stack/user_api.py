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
        user_api_v3 = apigwv2.HttpApi(
            scope,
            'UserApiV3',
            default_domain_mapping=apigwv2.DomainMappingOptions(
                domain_name=resources.sudocoins_domain_name,
                mapping_key='user'
            ),
            cors_preflight=self.default_cors_preflight
        )
        # GET PROFILE
        get_profile_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_profile_function
        )
        user_api_v3.add_routes(
            path='/profile',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_profile_integration
        )
        # UPDATE PROFILE
        update_profile_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.update_profile_function
        )
        user_api_v3.add_routes(
            path='/profile/update',
            methods=[apigwv2.HttpMethod.POST],
            integration=update_profile_integration
        )
        # USER VERIFY
        user_verify_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.user_verify_function
        )
        user_api_v3.add_routes(
            path='/verify',
            methods=[apigwv2.HttpMethod.POST],
            integration=user_verify_integration
        )
        # CASH OUT
        cash_out_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.cash_out_function
        )
        user_api_v3.add_routes(
            path='/cash-out',
            methods=[apigwv2.HttpMethod.POST],
            integration=cash_out_integration,
            authorizer=resources.sudocoins_authorizer
        )
        # MORE HISTORY
        more_history_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.more_history_function
        )
        user_api_v3.add_routes(
            path='/more-history',
            methods=[apigwv2.HttpMethod.POST],
            integration=more_history_integration
        )
        # CONTACT US
        contact_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.contact_function
        )
        user_api_v3.add_routes(
            path='/contact',
            methods=[apigwv2.HttpMethod.POST],
            integration=contact_integration
        )
