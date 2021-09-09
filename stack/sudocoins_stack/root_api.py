import typing
from resources import SudocoinsImportedResources
from root_lambdas import SudocoinsRootLambdas
from aws_cdk import (
    core as cdk,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations
)


class SudocoinsRootApi:

    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources,
                 lambdas: SudocoinsRootLambdas,
                 cors_preflight: typing.Optional[apigwv2.CorsPreflightOptions]):
        root_api_v1 = apigwv2.HttpApi(
            scope,
            'RootApiV1',
            default_domain_mapping=apigwv2.DomainMappingOptions(
                domain_name=resources.sudocoins_domain_name
            ),
            cors_preflight=cors_preflight
        )
        # SOCIAL SHARE
        social_share_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.social_share_function
        )
        root_api_v1.add_routes(
            path='/a/{shareId}',
            methods=[apigwv2.HttpMethod.GET],
            integration=social_share_integration
        )