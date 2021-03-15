from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This operator load data from AWS Redshift staging table to AWS Redshift dimension table. 
    This operator have 2 type insert mode: append and fresh_insert. append mode insert data without trancate,
    fresh_insert mode truncate before insert data.
    
    Parameters:
    target_table (string): target dimension table name
    redshift_conn_id (string): conn id of defined Redshift connection details on Airflow
    insert_mode (string): append and fresh_insert
    stage_table_select_query (string): this parameter is a sql select query for fetching data from staging table 
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 target_table=None,
                 redshift_conn_id="redshift",
                 insert_mode="append",
                 stage_table_select_query=None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.redshift_conn_id = redshift_conn_id
        self.insert_mode = insert_mode
        self.stage_table_select_query = stage_table_select_query
        self.insert_template = """INSERT INTO {}
                                {}
                               """

    def execute(self, context):
        self.log.info('Hook loading..')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info("Checking insert_mode for {}..".format(self.target_table))
        if self.insert_mode == "append":
            self.log.info("Detech append mode! Nothing to do before loading")
        elif self.insert_mode == "fresh_insert":
            self.log.info("Detech fresh_insert mode!")
            self.log.info("Dim table {} clear before load data..".format(self.target_table))
            redshift_hook.run("DELETE FROM {}".format(self.target_table))
        else:
            self.log.warn("Insert mode not specified!")
            raise
        
        self.log.info("Creating load script for {}..".format(self.target_table))
        insert_script = self.insert_template.format(self.target_table,
                                                    self.stage_table_select_query)
        
        self.log.info("Inser data from stage table to {}..".format(self.target_table))
        redshift_hook.run(insert_script)
            
        
