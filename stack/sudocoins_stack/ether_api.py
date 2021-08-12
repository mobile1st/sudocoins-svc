import typing
from resources import SudocoinsImportedResources
from ether_lambdas import SudocoinsEtherLambdas
from aws_cdk import (
    core as cdk,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations
)


class SudocoinsEtherApi:

    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources,
                 lambdas: SudocoinsEtherLambdas,
                 cors_preflight: typing.Optional[apigwv2.CorsPreflightOptions]):
        ether_api = apigwv2.HttpApi(
            scope,
            'EtherApi',
            default_domain_mapping=apigwv2.DomainMappingOptions(
                domain_name=resources.sudocoins_domain_name,
                mapping_key='ether'
            ),
            cors_preflight=cors_preflight
        )
