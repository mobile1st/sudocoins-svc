from resources import SudocoinsImportedResources
from aws_cdk import (
    core as cdk,
    aws_lambda as _lambda,
    aws_sns_subscriptions as subs,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_logs as logs
)

lambda_default_kwargs = {
    'runtime': _lambda.Runtime.PYTHON_3_8,
    'code': _lambda.Code.asset('../src'),
    'memory_size': 512,
    'log_retention': logs.RetentionDays.THREE_MONTHS
}


class SudocoinsAdminLambdas:
    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources):
        traffic_report_counter_store_function = _lambda.Function(
            scope,
            'AdminTrafficReportCounterStoreV2',
            function_name='AdminTrafficReportCounterStoreV2',
            handler='admin.traffic_report_counter_store.lambda_handler',
            **lambda_default_kwargs
        )
        resources.traffic_reports_table.grant_read_write_data(traffic_report_counter_store_function)
        resources.transaction_topic.add_subscription(
            subs.LambdaSubscription(traffic_report_counter_store_function)
        )
        self.traffic_report_chart_data_function = _lambda.Function(
            scope,
            'AdminTrafficReportChartDataV2',
            function_name='AdminTrafficReportChartDataV2',
            handler='admin.traffic_report_chart_data.lambda_handler',
            **lambda_default_kwargs
        )
        resources.traffic_reports_table.grant_read_data(self.traffic_report_chart_data_function)
        self.payouts_function = _lambda.Function(
            scope,
            'AdminPayoutsV2',
            function_name='AdminPayoutsV2',
            handler='admin.payouts.lambda_handler',
            **lambda_default_kwargs
        )
        resources.payouts_table.grant_read_data(self.payouts_function)
        resources.grant_read_index_data(self.payouts_function, [resources.payouts_table])
        self.user_details_function = _lambda.Function(
            scope,
            'AdminUserDetailsV2',
            function_name='AdminUserDetailsV2',
            handler='admin.user_details.lambda_handler',
            **lambda_default_kwargs
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
            handler='admin.update_cash_out.lambda_handler',
            **lambda_default_kwargs
        )
        resources.ledger_table.grant_read_data(self.update_cash_out_function)
        resources.payouts_table.grant_read_data(self.update_cash_out_function)
        # SET RATES
        set_rates_function = _lambda.Function(
            scope,
            'AdminSetRatesV2',
            function_name='AdminSetRatesV2',
            handler='admin.set_rates.lambda_handler',
            **lambda_default_kwargs
        )
        resources.config_table.grant_read_write_data(set_rates_function)
        set_rates_schedule = events.Schedule.rate(cdk.Duration.minutes(5))
        set_rates_target = events_targets.LambdaFunction(handler=set_rates_function)
        events.Rule(
            scope,
            'SetRatesRule',
            description='Call the BTC exchange rate function every 5 minutes',
            enabled=True,
            schedule=set_rates_schedule,
            targets=[set_rates_target]
        )
        # GET PENDING UPCOMING
        self.get_pending_upcoming_function = _lambda.Function(
            scope,
            'GetPendingUpcoming',
            function_name='GetPendingUpcoming',
            handler='admin.get_pending_upcoming.lambda_handler',
            **lambda_default_kwargs
        )
        resources.upcoming_table.grant_read_data(self.get_pending_upcoming_function)
        resources.grant_read_index_data(self.get_pending_upcoming_function, [resources.upcoming_table])
        # SET PENDING UPCOMING
        self.set_pending_upcoming_function = _lambda.Function(
            scope,
            'SetPendingUpcoming',
            function_name='SetPendingUpcoming',
            handler='admin.set_pending_upcoming.lambda_handler',
            **lambda_default_kwargs
        )
        resources.upcoming_table.grant_read_data(self.set_pending_upcoming_function)
        resources.grant_read_index_data(self.set_pending_upcoming_function, [resources.upcoming_table])