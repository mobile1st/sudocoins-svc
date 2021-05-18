from resources import SudocoinsImportedResources
from admin_lambdas import SudocoinsAdminLambdas
from aws_cdk import (
    core as cdk,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations
)


class SudocoinsAdminApi:
    default_cors_preflight = {
        'allow_methods': [apigwv2.CorsHttpMethod.ANY],
        'allow_origins': ['*'],
        'allow_headers': ['Content-Type', 'X-Amz-Date', 'Authorization', 'X-Api-Key', 'X-Amz-Security-Token',
                          'X-Amz-User-Agent']
    }

    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources,
                 lambdas: SudocoinsAdminLambdas):
        admin_api_v2 = apigwv2.HttpApi(
            scope,
            'AdminApiV2',
            cors_preflight=self.default_cors_preflight
        )
        # TRAFFIC REPORT
        traffic_report_chart_data_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.traffic_report_chart_data_function
        )
        admin_api_v2.add_routes(
            path='/traffic-report',
            methods=[apigwv2.HttpMethod.GET],
            integration=traffic_report_chart_data_integration,
            authorizer=resources.sudocoins_admin_authorizer
        )
        # PAYOUTS
        payouts_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.payouts_function
        )
        admin_api_v2.add_routes(
            path='/payouts',
            methods=[apigwv2.HttpMethod.GET],
            integration=payouts_integration,
            authorizer=resources.sudocoins_admin_authorizer
        )
        # USER DETAILS
        user_details_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.user_details_function
        )
        admin_api_v2.add_routes(
            path='/user/{userId}/details',
            methods=[apigwv2.HttpMethod.GET],
            integration=user_details_integration,
            authorizer=resources.sudocoins_admin_authorizer
        )
        # UPDATE CASH OUT
        update_cash_out_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.update_cash_out_function
        )
        admin_api_v2.add_routes(
            path='/cash-out',
            methods=[apigwv2.HttpMethod.POST],
            integration=update_cash_out_integration,
            authorizer=resources.sudocoins_admin_authorizer
        )