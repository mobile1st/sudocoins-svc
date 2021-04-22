from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigw,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations,
    aws_cognito as cognito
)

lambda_code_path = '../src'


class SudocoinsStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        traffic_reports_table = dynamodb.Table.from_table_arn(
            self, 'TrafficReportsTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/TrafficReports'
        )
        traffic_report_chart_data_function = _lambda.Function(
            self, 'AdminTrafficReportChartDataV2',
            function_name='AdminTrafficReportChartDataV2',
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='admin.traffic_report_chart_data.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        traffic_reports_table.grant_read_data(traffic_report_chart_data_function)

        # COGNITO
        sudocoins_admin_pool = cognito.UserPool.from_user_pool_id(self, 'SudoCoins-Admin', 'us-west-2_TpPw8Ed2z')
        sudocoins_admin_auth = apigw.CognitoUserPoolsAuthorizer(
            self, 'SudocoinsAdminAuthorizer',
            cognito_user_pools=[sudocoins_admin_pool]
        )
        # API GATEWAY V1
        self.build_admin_api_v1(traffic_report_chart_data_function, sudocoins_admin_auth)

        # API GATEWAY V2
        traffic_report_chart_data_integration = api_integrations.LambdaProxyIntegration(
            handler=traffic_report_chart_data_function
        )
        admin_api_v2 = apigwv2.HttpApi(
            self, 'AdminApiV2',
            cors_preflight={
                'allow_methods': [apigwv2.CorsHttpMethod.ANY],
                'allow_origins': ['*']
            }
        )
        admin_api_v2.add_routes(
            path="/trafficreport",
            methods=[apigwv2.HttpMethod.GET],
            integration=traffic_report_chart_data_integration
        )

    def build_admin_api_v1(self, traffic_report_chart_data_function, sudocoins_admin_auth):
        admin_api_v1 = apigw.RestApi(
            self, 'AdminApiV1',
            default_cors_preflight_options={
                'allow_origins': apigw.Cors.ALL_ORIGINS,
                'allow_methods': apigw.Cors.ALL_METHODS,
                'allow_headers': apigw.Cors.DEFAULT_HEADERS,
                'status_code': 200
            }
        )
        self.setup_gateway_response_header_for_cognito_auth(admin_api_v1)

        traffic_report_endpoint = admin_api_v1.root.add_resource('trafficreport');
        traffic_report_endpoint.add_method(
            'GET',
            apigw.LambdaIntegration(
                traffic_report_chart_data_function,
                proxy=False,
                integration_responses=[apigw.IntegrationResponse(
                    status_code='200',
                    response_templates={'application/json': ''},
                    response_parameters={'method.response.header.Access-Control-Allow-Origin': "'*'"}
                )],
                passthrough_behavior=apigw.PassthroughBehavior.WHEN_NO_TEMPLATES
            ),
            method_responses=[apigw.MethodResponse(
                status_code='200',
                response_models={'application/json': apigw.Model.EMPTY_MODEL},
                response_parameters={'method.response.header.Access-Control-Allow-Origin': True}
            )],
            authorizer=sudocoins_admin_auth,
            authorization_type=apigw.AuthorizationType.COGNITO
        )

    def setup_gateway_response_header_for_cognito_auth(self, api):
        apigw.CfnGatewayResponse(
            self, 'AccessDeniedResponse',
            response_type='ACCESS_DENIED',
            rest_api_id=api.rest_api_id,
            status_code='403',
            response_parameters={'gatewayresponse.header.Access-Control-Allow-Origin': "'*'"}
        )
        apigw.CfnGatewayResponse(
            self, 'DefaultBadRequestResponse',
            response_type='DEFAULT_4XX',
            rest_api_id=api.rest_api_id,
            response_parameters={'gatewayresponse.header.Access-Control-Allow-Origin': "'*'"}
        )
        apigw.CfnGatewayResponse(
            self, 'UnauthorizedResponse',
            response_type='UNAUTHORIZED',
            rest_api_id=api.rest_api_id,
            status_code='401',
            response_parameters={'gatewayresponse.header.Access-Control-Allow-Origin': "'*'"}
        )
