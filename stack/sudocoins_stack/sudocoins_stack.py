from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigw
)

lambda_code_path = '../src'


class SudocoinsStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        traffic_reports_table = dynamodb.Table.from_table_arn(self, 'TrafficReportsTable', 'arn:aws:dynamodb:us-west-2:977566059069:table/TrafficReports')
        traffic_report_chart_data = _lambda.Function(
            self, 'AdminTrafficReportChartDataV2',
            function_name='AdminTrafficReportChartDataV2',
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='admin.traffic_report_chart_data.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        traffic_reports_table.grant_read_data(traffic_report_chart_data)
