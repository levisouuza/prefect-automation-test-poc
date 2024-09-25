from service.s3_service import S3Service
from utils.date_utils import get_current_date_yyyymmdd
from model.config import Config
from model.parameters import Parameters
from prefect import flow, task
from time import sleep


class IngestionTestProcessor:
    def __init__(self, config: Config, params: Parameters):
        self._config = config
        self._params = params
        self._s3_service = S3Service(self._config, self._params)

    @flow(
        name="validate-new-ingestion",
        description="Flow to validate new ingestion table"
    )
    def ingestion_test_process(self):
        self._start_flow()
        self._upload_file_test.submit()
        self._change_params_ingestion.submit()
        self._check_transfer_files_between_bucket()

    @task(name="start_flow")
    def _start_flow(self):
        sleep(5)

    @task(name="upload_file_test")
    def _upload_file_test(self):
        self._s3_service.upload_file_s3(
            self._params.filepath,
            self._build_filename_s3()
        )

    @task(name="change_params_ingestion")
    def _change_params_ingestion(self):
        sleep(10)

    @task(name="check_transfer_files_between_bucket")
    def _check_transfer_files_between_bucket(self):
        sleep(5)

    def _build_filename_s3(self):
        filename = self._params.filepath.split("/")[-1]
        current_date = get_current_date_yyyymmdd()
        return (
            f"business/{self._params.provider}/"
            f"{current_date}/{filename}"
        )




