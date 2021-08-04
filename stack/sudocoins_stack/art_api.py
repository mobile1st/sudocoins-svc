import typing
from resources import SudocoinsImportedResources
from art_lambdas import SudocoinsArtLambdas
from aws_cdk import (
    core as cdk,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations
)


class SudocoinsArtApi:

    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources,
                 lambdas: SudocoinsArtLambdas,
                 cors_preflight: typing.Optional[apigwv2.CorsPreflightOptions]):
        art_api_v3 = apigwv2.HttpApi(
            scope,
            'ArtApiV3',
            default_domain_mapping=apigwv2.DomainMappingOptions(
                domain_name=resources.sudocoins_domain_name,
                mapping_key='art'
            ),
            cors_preflight=cors_preflight
        )
        # ADD ART
        add_art_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.add_art_function
        )
        art_api_v3.add_routes(
            path='/',
            methods=[apigwv2.HttpMethod.POST],
            integration=add_art_integration
        )
        # SHARE ART
        share_art_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.share_art_function
        )
        art_api_v3.add_routes(
            path='/share',
            methods=[apigwv2.HttpMethod.POST],
            integration=share_art_integration
        )
        # ART SOURCE REDIRECT
        art_source_redirect_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.art_source_redirect_function
        )
        art_api_v3.add_routes(
            path='/source',
            methods=[apigwv2.HttpMethod.GET],
            integration=art_source_redirect_integration
        )
        # GET ARTS
        get_arts_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_arts_function
        )
        art_api_v3.add_routes(
            path='/arts',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_arts_integration
        )
        # GET RECENT ARTS
        get_recent_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_recent_function
        )
        art_api_v3.add_routes(
            path='/recent',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_recent_integration
        )
        # GET TRENDING ARTS
        get_trending_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_trending_function
        )
        art_api_v3.add_routes(
            path='/trending',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_trending_integration
        )
        # GET USER ARTS
        get_user_arts_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_user_arts_function
        )
        art_api_v3.add_routes(
            path='/user/{userId}',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_user_arts_integration
        )
        # GET SHARED ART
        get_shared_art_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_shared_art_function
        )
        art_api_v3.add_routes(
            path='/share/{shareId}',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_shared_art_integration
        )
        # INCREMENT VIEW COUNT
        increment_view_count_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.increment_view_count_function
        )
        art_api_v3.add_routes(
            path='/increment-view',
            methods=[apigwv2.HttpMethod.POST],
            integration=increment_view_count_integration
        )
        # GET LEADERBOARD
        get_leaderboard_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_leaderboard_function
        )
        art_api_v3.add_routes(
            path='/leaderboard',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_leaderboard_integration
        )
        # ADD VOTE
        add_vote_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.add_vote_function
        )
        art_api_v3.add_routes(
            path='/vote',
            methods=[apigwv2.HttpMethod.GET],
            integration=add_vote_integration
        )
        # GET PREVIEW
        get_preview_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_preview_function
        )
        art_api_v3.add_routes(
            path='/social/{shareId}',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_preview_integration
        )
        # ART SEARCH
        art_search_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.art_search_function
        )
        art_api_v3.add_routes(
            path='/search',
            methods=[apigwv2.HttpMethod.GET],
            integration=art_search_integration
        )
        # UPDATE TAGS
        update_tags_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.update_tags_function
        )
        art_api_v3.add_routes(
            path='/update-tags',
            methods=[apigwv2.HttpMethod.GET],
            integration=update_tags_integration
        )
