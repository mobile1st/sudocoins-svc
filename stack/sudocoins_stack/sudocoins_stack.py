from resources import SudocoinsImportedResources
from admin_lambdas import SudocoinsAdminLambdas
from art_lambdas import SudocoinsArtLambdas
from art_processor_lambdas import SudocoinsArtProcessorLambdas
from user_lambdas import SudocoinsUserLambdas
from admin_api import SudocoinsAdminApi
from art_api import SudocoinsArtApi
from user_api import SudocoinsUserApi
from aws_cdk import (
    core as cdk,
    aws_apigatewayv2 as apigwv2,
)

default_cors_preflight = {
    'allow_methods': [apigwv2.CorsHttpMethod.ANY],
    'allow_origins': ['*'],
    'allow_headers': ['Content-Type', 'X-Amz-Date', 'Authorization', 'X-Api-Key', 'X-Amz-Security-Token',
                      'X-Amz-User-Agent', 'sub']
}


class SudocoinsStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        resources = SudocoinsImportedResources(self)
        admin_lambdas = SudocoinsAdminLambdas(self, resources)
        admin_api = SudocoinsAdminApi(self, resources, admin_lambdas, default_cors_preflight)
        art_lambdas = SudocoinsArtLambdas(self, resources)
        art_processor_lambdas = SudocoinsArtProcessorLambdas(self, resources)
        art_api = SudocoinsArtApi(self, resources, art_lambdas, default_cors_preflight)
        user_lambdas = SudocoinsUserLambdas(self, resources)
        user_api = SudocoinsUserApi(self, resources, user_lambdas, default_cors_preflight)