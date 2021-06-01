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
        # SHARE ART
        share_art_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.share_art_function
        )
        art_api_v2.add_routes(
            path='/art/share',
            methods=[apigwv2.HttpMethod.POST],
            integration=share_art_integration
        )
        # ART REDIRECT - GO TO SOURCE
        art_redirect_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.art_redirect_function
        )
        art_api_v2.add_routes(
            path='/art/source',
            methods=[apigwv2.HttpMethod.GET],
            integration=art_redirect_integration
        )
        # GET ARTS
        get_arts_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_arts_function
        )
        art_api_v2.add_routes(
            path='/arts',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_arts_integration
        )
        # GET RECENT ARTS
        get_recent_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_recent_function
        )
        art_api_v2.add_routes(
            path='/arts/recent',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_recent_integration
        )
        # GET TRENDING ARTS
        get_trending_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_trending_function
        )
        art_api_v2.add_routes(
            path='/arts/trending',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_trending_integration
        )
        # GET USER ARTS
        my_gallery_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.my_gallery_function
        )
        art_api_v2.add_routes(
            path='/arts/user/{userId}',
            methods=[apigwv2.HttpMethod.GET],
            integration=my_gallery_integration
        )
        # ART PROMPT
        art_prompt_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.art_prompt_function
        )
        art_api_v2.add_routes(
            path='/art/share/{shareId}',
            methods=[apigwv2.HttpMethod.GET],
            integration=art_prompt_integration
        )
        # ADD VIEW
        add_view_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.add_view_function
        )
        art_api_v2.add_routes(
            path='/art/increment-view',
            methods=[apigwv2.HttpMethod.POST],
            integration=add_view_integration
        )
