from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This operator load data from AWS Redshift staging table to AWS Redshift fact table. 
    
    Parameters:
    target_table (string): target dimension table name
    redshift_conn_id (string): conn id of defined Redshift connection details on Airflow
    stage_table_select_query (string): this parameter is a sql select query for fetching data from staging table 
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 target_table=None,
                 redshift_conn_id="redshift",
                 stage_table_select_query=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.redshift_conn_id = redshift_conn_id
        self.stage_table_select_query = stage_table_select_query
        self.insert_template = """INSERT INTO {}
                                {}
                             """

    def execute(self, context):
        self.log.info('Hook loading..')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info("Creating load script for {}..".format(self.target_table))
        insert_script = self.insert_template.format(self.target_table,
                                                    self.stage_table_select_query)

        self.log.info("Inser data from stage table to {}..".format(self.target_table))
        redshift_hook.run(insert_script)