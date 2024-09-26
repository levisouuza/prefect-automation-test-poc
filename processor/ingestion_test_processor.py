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
        result_start_flow = self._start_flow()
        result_upload = self._upload_file_test.submit(result_start_flow)
        result_change_params = self._change_params_ingestion.submit(result_start_flow)

        result_upload.result()
        result_change_params.result()

        result_execute_lambda_start_task = self._execute_lambda_start_task.submit(
            result_upload, result_change_params
        )

        result_execute_lambda_start_task.result()

        result_check_transfer = self._check_transfer_files_between_bucket.submit(
            result_execute_lambda_start_task
        )

        result_check_transfer.result()

        result_glue_job = self._check_glue_job_execution.submit(result_check_transfer)
        result_glue_job.result()

        result_validate_file_existence = self._validate_existence_file_bucket_sor.submit(
            result_glue_job
        )

        result_execute_athena_counter_query = self._execute_athena_counter_query.submit(
            result_glue_job
        )

        result_viewer_sample_athena_table = self._viewer_sample_athena_table.submit(
            result_glue_job
        )

        result_validate_file_existence.result()
        result_execute_athena_counter_query.result()
        result_viewer_sample_athena_table.result()

        result_finish_flow = self._finish_flow.submit(
            result_validate_file_existence,
            result_execute_athena_counter_query,
            result_viewer_sample_athena_table
        )

        result_finish_flow.result()

    @task(name="start_flow")
    def _start_flow(self):
        return 'Start Flow'

    @task(name="upload_file_test")
    def _upload_file_test(self, _result_before_task):
        self._s3_service.upload_file_s3(
            self._params.filepath,
            self._build_filename_s3()
        )
        return True

    @task(name="change_params_ingestion")
    def _change_params_ingestion(self, _result_before_task):
        sleep(2)
        return True

    @task(name="execute_lambda_start_task")
    def _execute_lambda_start_task(
            self, _result_before_task1, _result_before_task2
    ):
        sleep(1)
        return True

    @task(name="check_transfer_files_between_bucket")
    def _check_transfer_files_between_bucket(self, _result_before_task):
        sleep(2)
        return True

    @task(name="check_glue_job_execution")
    def _check_glue_job_execution(self, _result_before_task):
        sleep(2)
        return True

    @task(name="validate_existence_file_bucket_sor")
    def _validate_existence_file_bucket_sor(self, _result_before_task):
        sleep(2)
        return True

    @task(name="execute_athena_counter_query")
    def _execute_athena_counter_query(self, _result_before_task):
        sleep(2)
        return True

    @task(name="return_viewer_sample_athena_table")
    def _viewer_sample_athena_table(self, _result_before_task):
        sleep(2)
        return True

    @task(name="finish_flow")
    def _finish_flow(
            self, _result_before_task1, _result_before_task2, _result_before_task3
    ):
        return "Finish Flow"

    def _build_filename_s3(self):
        filename = self._params.filepath.split("/")[-1]
        current_date = get_current_date_yyyymmdd()
        return (
            f"business/{self._params.provider}/"
            f"{current_date}/{filename}"
        )




