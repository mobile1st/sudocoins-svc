import typing
from resources import SudocoinsImportedResources
from admin_lambdas import SudocoinsAdminLambdas
from aws_cdk import (
    core as cdk,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations
)


class SudocoinsAdminApi:

    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources,
                 lambdas: SudocoinsAdminLambdas,
                 cors_preflight: typing.Optional[apigwv2.CorsPreflightOptions]):
        admin_api_v2 = apigwv2.HttpApi(
            scope,
            'AdminApiV2',
            default_domain_mapping=apigwv2.DomainMappingOptions(
                domain_name=resources.sudocoins_domain_name,
                mapping_key='admin'
            ),
            cors_preflight=cors_preflight
        )
