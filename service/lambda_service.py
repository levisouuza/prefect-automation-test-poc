import json

from model.config import Config
from model.parameters import Parameters
from service.aws_service import AwsService
from botocore.client import ClientError


class LambdaService:
    def __init__(self, config: Config, params: Parameters):
        self._client = AwsService.get_client(config, "lambda")
        self._params = params

    def invoke_function(self, payload: dict):

        payload_to_bytes = json.dumps(payload).encode('utf-8')
        try:
            response = self._client.invoke(
                FunctionName=self._params.lambda_name,
                InvocationType='Event',
                Payload=payload_to_bytes
            )

            return response.get("StatusCode")

        except ClientError as err:
            raise Exception(
                f"Error while invoke lambda function: {str(err)}"
            )

    @staticmethod
    def check_status_invoke_function(status_code):
        return True if status_code == 200 else False
