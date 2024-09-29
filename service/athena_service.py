from time import sleep
from model.config import Config
from model.parameters import Parameters
from service.aws_service import AwsService
from botocore.client import ClientError


class AthenaService:
    def __init__(self, config: Config, params: Parameters):
        self._client = AwsService.get_client(config, "athena")
        self._params = params

    def get_value_from_athena_query(self, query):
        query_id = self._execute_query_athena(query)
        status_query, query_id = self._check_status_query(query_id)
        if status_query == 'SUCCEEDED':
            results = self._client.get_query_results(QueryExecutionId=query_id)

            for row in results['ResultSet']['Rows']:
                print([col['VarCharValue'] for col in row['Data']])

        if status_query in ['FAILED', 'CANCELLED']:
            raise Exception("Execute query with error")

    def _check_status_query(self, query_execution_id):
        while True:
            query_status = self._get_query_execution(query_execution_id)
            status = query_status['QueryExecution']['Status']['State']

            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                print(f'Consulta finalizada com o status: {status}')
                break
            else:
                print('Aguardando a finalização da consulta...')
                sleep(2)

        return status, query_execution_id

    def _execute_query_athena(self, query):
        output_location = f"s3://{self._params.bucket_stage}/result_query_athena/"
        try:
            response = self._client.start_query_execution(
                QueryString=query,
                ResultConfiguration={
                    'OutputLocation': output_location
                }
            )
            return response.get('QueryExecutionId')

        except ClientError as err:
            raise Exception(f"Query execute with error: {err}")

    def _get_query_execution(self, query_id):
        try:
            query_status = self._client.get_query_execution(
                QueryExecutionId=query_id
            )
            return query_status

        except ClientError as err:
            raise Exception(f"Get query execution with error: {err}")
