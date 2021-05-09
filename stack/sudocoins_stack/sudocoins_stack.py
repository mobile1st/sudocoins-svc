from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations,
    aws_apigatewayv2_authorizers as api_authorizers,
    aws_cognito as cognito,
    aws_iam as iam
)

lambda_code_path = '../src'


class SudocoinsStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        resources = SudocoinsImportedResources(self)
        admin_lambdas = SudocoinsAdminLambdas(self, resources)
        admin_api = SudocoinsAdminApi(self, resources, admin_lambdas)


class SudocoinsImportedResources:
    def __init__(self, scope: cdk.Construct):
        self.traffic_reports_table = dynamodb.Table.from_table_arn(
            scope,
            'TrafficReportsTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/TrafficReports'
        )
        self.payouts_table = dynamodb.Table.from_table_arn(
            scope,
            'PayoutsTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/Payouts'
        )
        self.ledger_table = dynamodb.Table.from_table_arn(
            scope,
            'LedgerTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/Ledger'
        )
        self.transaction_table = dynamodb.Table.from_table_arn(
            scope,
            'TransactionTable',
            'arn:aws:dynamodb:us-west-2:977566059069:table/Transaction'
        )
        self.sudocoins_admin_authorizer = self.init_admin_authorizer(scope)

    def init_admin_authorizer(self, scope: cdk.Construct):
        sudocoins_admin_pool = cognito.UserPool.from_user_pool_id(scope, 'SudoCoins-Admin', 'us-west-2_TpPw8Ed2z')
        sudocoins_admin_ui_client = cognito.UserPoolClient.from_user_pool_client_id(
            scope,
            'SudocoinsAdminUIClient',
            '1emc1ko93cb7priri26dtih1pq'
        )
        return api_authorizers.HttpUserPoolAuthorizer(
            user_pool=sudocoins_admin_pool,
            user_pool_client=sudocoins_admin_ui_client
        )


class SudocoinsAdminLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        self.traffic_report_chart_data_function = _lambda.Function(
            scope,
            'AdminTrafficReportChartDataV2',
            function_name='AdminTrafficReportChartDataV2',
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='admin.traffic_report_chart_data.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.traffic_reports_table.grant_read_data(self.traffic_report_chart_data_function)
        self.payouts_function = _lambda.Function(
            scope,
            'AdminPayoutsV2',
            function_name='AdminPayoutsV2',
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='admin.payouts.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.payouts_table.grant_read_data(self.payouts_function)
        self.payouts_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=['arn:aws:dynamodb:us-west-2:977566059069:table/Payouts/index/*'],
                actions=['dynamodb:Query']
            )
        )
        self.user_details_function = _lambda.Function(
            scope,
            'AdminUserDetailsV2',
            function_name='AdminUserDetailsV2',
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='admin.user_details.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.ledger_table.grant_read_data(self.user_details_function)
        resources.transaction_table.grant_read_data(self.user_details_function)
        self.user_details_function.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[
                    'arn:aws:dynamodb:us-west-2:977566059069:table/Ledger/index/*',
                    'arn:aws:dynamodb:us-west-2:977566059069:table/Transaction/index/*'
                ],
                actions=['dynamodb:Query']
            )
        )
        self.update_cash_out_function = _lambda.Function(
            scope,
            'AdminUpdateCashOutV2',
            function_name='AdminUpdateCashOutV2',
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='admin.update_cash_out.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.ledger_table.grant_read_data(self.update_cash_out_function)
        resources.payouts_table.grant_read_data(self.update_cash_out_function)


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
            path='/user/details',
            methods=[apigwv2.HttpMethod.POST],
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
