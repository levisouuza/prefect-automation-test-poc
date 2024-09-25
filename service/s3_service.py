from model.config import Config
from model.parameters import Parameters
from utils.date_utils import get_current_date_yyyymmdd
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
                self._params.src_bucket,
                name_object_s3
            )

        except ClientError as err:
            print(f"Error while upload file: {local_filename} -> {str(err)}")


    def check_file_s3(self, filepath: str):
        pass



# _CONFIG = Config(
#     aws_access_key_id="AKIAZINR3TSSEAAAB56C",
#     aws_secret_access_key="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",
# )
#
# _PARAMS = Parameters(
#     src_bucket="development-test-levis-external"
# )
#
# _s3_service = S3Service(_CONFIG, _PARAMS)
#
# current_date = get_current_date_yyyymmdd()
# _s3_service.upload_file_s3("../movies.csv", f"business/imdb/{current_date}/movies.csv")
