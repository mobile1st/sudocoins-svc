import typing
from resources import SudocoinsImportedResources
from user_lambdas import SudocoinsUserLambdas
from aws_cdk import (
    core as cdk,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as api_integrations
)


class SudocoinsUserApi:

    def __init__(self,
                 scope: cdk.Construct,
                 resources: SudocoinsImportedResources,
                 lambdas: SudocoinsUserLambdas,
                 cors_preflight: typing.Optional[apigwv2.CorsPreflightOptions]):
        user_api_v3 = apigwv2.HttpApi(
            scope,
            'UserApiV3',
            default_domain_mapping=apigwv2.DomainMappingOptions(
                domain_name=resources.sudocoins_domain_name,
                mapping_key='user'
            ),
            cors_preflight=cors_preflight
        )

        # GET PROFILE DEV
        get_profile_dev_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_profile_dev_function
        )
        user_api_v3.add_routes(
            path='/profileDev',
            methods=[apigwv2.HttpMethod.POST],
            integration=get_profile_dev_integration
        )
        # GET USERID FOR META USER
        get_user_id_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_user_id_function
        )
        user_api_v3.add_routes(
            path='/userId/{publicAddress}',
            methods=[apigwv2.HttpMethod.GET],
            integration=get_user_id_integration
        )

        # CONTACT US
        contact_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.contact_function
        )
        user_api_v3.add_routes(
            path='/contact',
            methods=[apigwv2.HttpMethod.POST],
            integration=contact_integration
        )
        # GET TWITTER TOKEN
        get_twitter_token_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_twitter_token_function
        )
        user_api_v3.add_routes(
            path="/twitter/request_token",
            methods=[apigwv2.HttpMethod.GET],
            integration=get_twitter_token_integration
        )
        # SET PORTFOLIO
        set_portfolio_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.set_portfolio_function
        )
        user_api_v3.add_routes(
            path="/set-portfolio",
            methods=[apigwv2.HttpMethod.POST],
            integration=set_portfolio_integration
        )
        # GET PORTFOLIO
        get_portfolio_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.get_portfolio_function
        )
        user_api_v3.add_routes(
            path="/get-portfolio",
            methods=[apigwv2.HttpMethod.POST],
            integration=get_portfolio_integration
        )
        # IMPORT NFTs
        import_nfts_integration = api_integrations.LambdaProxyIntegration(
            handler=lambdas.import_nfts_function
        )
        user_api_v3.add_routes(
            path="/import-nfts",
            methods=[apigwv2.HttpMethod.POST],
            integration=import_nfts_integration
        )

