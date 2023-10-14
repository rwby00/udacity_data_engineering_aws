from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 tests=None,  # A list of custom test SQL queries, optional
                 expected_results=None,  # Expected results for the tests, optional
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.tests = tests
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.tables:
            raise ValueError("No tables provided")

        for table in self.tables:
            self.log.info(f"Checking data quality for {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table} has no records.")
            self.log.info(f"Data quality check passed for {table} with {records[0][0]} records")

        if self.tests and self.expected_results:
            if len(self.tests) != len(self.expected_results):
                raise ValueError("Mismatch between number of tests and expected results")

            for i, test in enumerate(self.tests):
                records = redshift.get_records(test)
                if records[0][0] != self.expected_results[i]:
                    raise ValueError(f"Data quality test failed for test: {test}, got: {records[0][0]}, expected: {self.expected_results[i]}")
                self.log.info(f"Data quality test passed for {test}")
