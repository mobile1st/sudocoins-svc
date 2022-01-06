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
            methods=[apigwv2.HttpMethod.POST],
            integration=update_tags_integration
        )
        # GET HEARTS
        get_hearts_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_hearts_function
        )
        art_api_v3.add_routes(
            path='/get-hearts',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_hearts_integration
        )
        # START MINT
        start_mint_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.start_mint_function
        )
        art_api_v3.add_routes(
            path='/start-mint',
            methods=[apigwv2.HttpMethod.POST],
            integration=start_mint_integration
        )
        # SET IPFS
        set_ipfs_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.set_ipfs_function
        )
        art_api_v3.add_routes(
            path='/set-ipfs',
            methods=[apigwv2.HttpMethod.POST],
            integration=set_ipfs_integration
        )
        # GET MINT REQUEST
        get_mint_request_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_mint_function
        )
        art_api_v3.add_routes(
            path='/get-mint',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_mint_request_integration
        )
        # END MINT REQUEST
        end_mint_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.end_mint_function
        )
        art_api_v3.add_routes(
            path='/end-mint',
            methods=[apigwv2.HttpMethod.POST],
            integration=end_mint_integration
        )
        # COLLECTION NAME
        collection_name_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.collection_name_function
        )
        art_api_v3.add_routes(
            path='/collection-name',
            methods=[apigwv2.HttpMethod.POST],
            integration=collection_name_integration
        )
        # DELETE ART
        delete_art_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.delete_art_function
        )
        art_api_v3.add_routes(
            path='/delete-art',
            methods=[apigwv2.HttpMethod.POST],
            integration=delete_art_integration
        )
        # GET MINTED
        get_minted_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_minted_function
        )
        art_api_v3.add_routes(
            path='/get-minted',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_minted_integration
        )
        # COLLECTION PAGE
        collection_page_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.collection_page_function
        )
        art_api_v3.add_routes(
            path='/collection-page',
            methods=[apigwv2.HttpMethod.POST],
            integration=collection_page_integration
        )
        # TOP BUYERS PAGE
        top_buyers_page_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.top_buyers_page_function
        )
        art_api_v3.add_routes(
            path='/top-buyers-page',
            methods=[apigwv2.HttpMethod.POST],
            integration=top_buyers_page_integration
        )
        # GET TOP BUYERS
        get_buyers_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_buyers_function
        )
        art_api_v3.add_routes(
            path='/get-buyers',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_buyers_integration
        )
        # GET NEW COLLECTIONS
        get_new_collections_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_new_collections_function
        )
        art_api_v3.add_routes(
            path='/new-collections',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_new_collections_integration
        )
        # ADD CHAT
        add_chat_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.add_chat_function
        )
        art_api_v3.add_routes(
            path='/add-chat',
            methods=[apigwv2.HttpMethod.POST],
            integration=add_chat_integration
        )
        # GET CHATS
        get_chats_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_chats_function
        )
        art_api_v3.add_routes(
            path='/get-chats',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_chats_integration
        )
        # GET UPCOMING
        get_upcoming_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_upcoming_function
        )
        art_api_v3.add_routes(
            path='/get-upcoming',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_upcoming_integration
        )
        # ADD UPCOMING
        add_upcoming_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.add_upcoming_function
        )
        art_api_v3.add_routes(
            path='/add-upcoming',
            methods=[apigwv2.HttpMethod.POST],
            integration=add_upcoming_integration
        )
        # GET RELATED
        get_related_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_related_function
        )
        art_api_v3.add_routes(
            path='/get-related',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_related_integration
        )
        # GET TOP COLLECTIONS
        get_top_collections_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_top_collections_function
        )
        art_api_v3.add_routes(
            path='/top-collections',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_top_collections_integration
        )
        # GET TRADES DELTA
        get_trades_delta_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_trades_delta_function
        )
        art_api_v3.add_routes(
            path='/trades-delta',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_trades_delta_integration
        )
        # GET NEWS
        get_news_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_news_function
        )
        art_api_v3.add_routes(
            path='/get-news',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_news_integration
        )
        # GET Floor DELTA
        get_floor_delta_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_floor_delta_function
        )
        art_api_v3.add_routes(
            path='/floor-delta',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_floor_delta_integration
        )
        # GET Median DELTA
        get_median_delta_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_median_delta_function
        )
        art_api_v3.add_routes(
            path='/median-delta',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_median_delta_integration
        )
        # GET Volume DELTA
        get_volume_delta_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_volume_delta_function
        )
        art_api_v3.add_routes(
            path='/volume-delta',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_volume_delta_integration
        )


