from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_checks=None,
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_checks = table_checks or []
        self.dq_checks = dq_checks or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table_check in self.table_checks:
            table_name = table_check['table']
            min_records = table_check.get('min_records', 1)  # Default to 1 if not provided
            self.log.info(f"Checking data quality for {table_name}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table_name}")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < min_records:
                raise ValueError(f"Data quality check failed. {table_name} had less than {min_records} records.")
            self.log.info(f"Data quality check passed for {table_name} with {records[0][0]} records")

        for check in self.dq_checks:
            check_sql = check['check_sql']
            expected_result = check['expected_result']
            
            records = redshift.get_records(check_sql)
            if records[0][0] != expected_result:
                raise ValueError(f"Data quality check failed. SQL: {check_sql}. Got: {records[0][0]}, expected: {expected_result}")
            self.log.info(f"Data quality check passed for SQL: {check_sql}")



