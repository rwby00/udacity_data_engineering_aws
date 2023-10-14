from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    # SQL template for inserting data into Redshift
    insert_sql_template = """
        INSERT INTO {table}
        {select_sql};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  # Connection ID for Redshift
                 table="",             # Target fact table in Redshift
                 select_sql="",        # SQL query for selecting data
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info(f"Loading fact table {self.table} in Redshift")
        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        formatted_sql = LoadFactOperator.insert_sql_template.format(
            table=self.table,
            select_sql=self.select_sql
        )
        
        self.log.info(f"Executing {formatted_sql}")
        redshift_hook.run(formatted_sql)
        self.log.info(f"Loaded fact table {self.table} in Redshift")
