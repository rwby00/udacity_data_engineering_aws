from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 insert_mode="overwrite",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.insert_mode == "overwrite":
            self.log.info(f"Clearing data from Redshift table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")
            
        self.log.info(f"Loading data into {self.table} dimension table in Redshift")
        insert_sql = f"INSERT INTO {self.table} {self.select_sql}"
        redshift.run(insert_sql)
