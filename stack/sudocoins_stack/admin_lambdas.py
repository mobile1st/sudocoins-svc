from resources import SudocoinsImportedResources
from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_sns_subscriptions as subs
)

lambda_code_path = '../src'
lambda_runtime = _lambda.Runtime.PYTHON_3_8


class SudocoinsAdminLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        traffic_report_counter_store_function = _lambda.Function(
            scope,
            'AdminTrafficReportCounterStoreV2',
            function_name='AdminTrafficReportCounterStoreV2',
            runtime=lambda_runtime,
            handler='admin.traffic_report_counter_store.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.traffic_reports_table.grant_read_write_data(traffic_report_counter_store_function)
        resources.transaction_topic.add_subscription(
            subs.LambdaSubscription(traffic_report_counter_store_function)
        )
        self.traffic_report_chart_data_function = _lambda.Function(
            scope,
            'AdminTrafficReportChartDataV2',
            function_name='AdminTrafficReportChartDataV2',
            runtime=lambda_runtime,
            handler='admin.traffic_report_chart_data.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.traffic_reports_table.grant_read_data(self.traffic_report_chart_data_function)
        self.payouts_function = _lambda.Function(
            scope,
            'AdminPayoutsV2',
            function_name='AdminPayoutsV2',
            runtime=lambda_runtime,
            handler='admin.payouts.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.payouts_table.grant_read_data(self.payouts_function)
        resources.grant_read_index_data(self.payouts_function, [resources.payouts_table])
        self.user_details_function = _lambda.Function(
            scope,
            'AdminUserDetailsV2',
            function_name='AdminUserDetailsV2',
            runtime=lambda_runtime,
            handler='admin.user_details.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.ledger_table.grant_read_data(self.user_details_function)
        resources.transaction_table.grant_read_data(self.user_details_function)
        resources.grant_read_index_data(
            self.user_details_function,
            [resources.transaction_table, resources.ledger_table]
        )
        self.update_cash_out_function = _lambda.Function(
            scope,
            'AdminUpdateCashOutV2',
            function_name='AdminUpdateCashOutV2',
            runtime=lambda_runtime,
            handler='admin.update_cash_out.lambda_handler',
            code=_lambda.Code.asset(lambda_code_path)
        )
        resources.ledger_table.grant_read_data(self.update_cash_out_function)
        resources.payouts_table.grant_read_data(self.update_cash_out_function)
