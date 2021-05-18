from resources import SudocoinsImportedResources
from admin_lambdas import SudocoinsAdminLambdas
from survey_lambdas import SudocoinsSurveyLambdas
from admin_api import SudocoinsAdminApi
from survey_api import SudocoinsSurveyApi
from aws_cdk import (
    core as cdk
)


class SudocoinsStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        resources = SudocoinsImportedResources(self)
        admin_lambdas = SudocoinsAdminLambdas(self, resources)
        admin_api = SudocoinsAdminApi(self, resources, admin_lambdas)
        survey_lambdas = SudocoinsSurveyLambdas(self, resources)
        survey_api = SudocoinsSurveyApi(self, resources, survey_lambdas)
