from resources import SudocoinsImportedResources
from art_lambdas import SudocoinsArtLambdas
from aws_cdk import (
    core as cdk,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations
)


class SudocoinsArtApi:
    default_cors_preflight = {
        'allow_methods': [apigwv2.CorsHttpMethod.ANY],
        'allow_origins': ['*'],
        'allow_headers': ['Content-Type', 'X-Amz-Date', 'Authorization', 'X-Api-Key', 'X-Amz-Security-Token',
                          'X-Amz-User-Agent']
    }

    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources,
                 lambdas: SudocoinsArtLambdas):
        art_api_v2 = apigwv2.HttpApi(
            scope,
            'ArtApiV2',
            cors_preflight=self.default_cors_preflight
        )
        # ADD ART
        add_art_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.add_art_function
        )
        art_api_v2.add_routes(
            path='/art',
            methods=[apigwv2.HttpMethod.POST],
            integration=add_art_integration
        )
        # ART PROMPT
        art_prompt_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.art_prompt_function
        )
        art_api_v2.add_routes(
            path='/art/{shareId}',
            methods=[apigwv2.HttpMethod.GET], # previously it was POST
            integration=art_prompt_integration
        )
