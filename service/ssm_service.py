from model.config import Config
from model.parameters import Parameters
from service.aws_service import AwsService
from botocore.client import ClientError


class SsmService:
    def __init__(self, config: Config, params: Parameters):
        self._client = AwsService.get_client(config, "ssm")
        self._params = params

    def delete_params_to_test(self):
        try:
            self._client.delete_parameter(
                Name=f"{self._build_path_params_name()}/_test"
            )
        except ClientError as err:
            raise Exception(f"Delete Parameter test Failed: {str(err)}")

    def get_parameter(self):
        try:
            response = self._client.get_parameter(
                Name=self._build_path_params_name()
            )
            return response.get("Parameter").get("Value")
        except ClientError as err:
            raise Exception(f"Parameter not Found: {str(err)}")

    def put_parameter(self, params_value_to_test: str):
        try:
            self._client.put_parameter(
                Name=f"{self._build_path_params_name()}/_test",
                Description='string',
                Value=params_value_to_test,
                Overwrite=True,
                Type='String',
                Tier='Standard'
            )
            return 200

        except ClientError as err:
            raise ValueError(f"Put Parameter test Failed: {str(err)}")

    def _build_path_params_name(self):
        return f"/stage/{self._params.provider}/{self._params.business}"

#
# _CONFIG = Config(
#     aws_access_key_id="AKIAZINR3TSSEAAAB56C",
#     aws_secret_access_key="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",
# )
#
# _PARAMS = Parameters(
#     external_bucket="development-test-levis-external",
#     provider="imdb",
#     business="movies"
# )
#
# _ssm_service = SsmService(_CONFIG, _PARAMS)
# params_value = _ssm_service.get_parameter()
# print(params_value)
