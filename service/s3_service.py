from time import sleep
from model.config import Config
from model.parameters import Parameters
from service.aws_service import AwsService
from botocore.client import ClientError


class S3Service:
    def __init__(self, config: Config, params: Parameters):
        self._client = AwsService.get_client(config, "s3")
        self._params = params

    def upload_file_s3(self, local_filename: str, s3_filename=None):
        name_object_s3 = s3_filename if s3_filename else local_filename
        try:
            self._client.upload_file(
                local_filename,
                self._params.bucket_external,
                name_object_s3
            )

        except ClientError as err:
            raise Exception(f"Error while upload file: {local_filename} -> {str(err)}")

    def check_existence_file_s3(self, bucket: str, key: str, time: int):
        counter = 1
        while counter <= 5:
            try:
                return self._get_object_in_bucket(bucket, key)

            except Exception as err:
                print(f"File not found. Key: {key}. Error: {err}")
                print(f"Current Counter: {counter}. Add one more")
                counter += 1
                sleep(time)

        raise Exception("Error in transfer file.")

    def _get_object_in_bucket(self, bucket: str, key: str):
        resp = self._client.get_object(
            Bucket=bucket, Key=key
        )
        return resp.get("ResponseMetadata").get("HTTPStatusCode")


