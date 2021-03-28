from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This operator check null value for given tables and fields on AWS Redshift. 
    
    Parameters:
    control_tables_and_fields (dict): key: table_name, value:check_field  
    redshift_conn_id (string): conn id of defined Redshift connection details on Airflow
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 control_tables_and_fields={},
                 redshift_conn_id="redshift",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.control_tables_and_fields = control_tables_and_fields
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Hook loading..')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info('Data Quality Checking..')
        for table_name in self.control_tables_and_fields.keys(): 
            self.log.info('Checking {} table data quality on {} field..'.format(table_name, self.control_tables_and_fields[table_name]))
            check_script = "SELECT COUNT(*) FROM {} WHERE {} IS NULL".format(table_name, self.control_tables_and_fields[table_name])
            
            records = redshift_hook.get_records(check_script)
            if records[0][0] > 0:
                raise
            else:
                self.log.info('Data Quality is OK..')