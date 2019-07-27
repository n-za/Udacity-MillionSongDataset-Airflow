from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table = '',
                 *args, **kwargs):
        """
            Constructor:
        
            * redshift_conn_id a connection to redshift (It would be a bad idea to reconnect for each query)
            * the table to be processed
            * generic arguments and keyword arguments
        """
    
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        """
            Processor: It inserts into the table given in the 'table' field.
            
        """
        
        self.log.info('LoadFactOperator ({}) running:'.format(self.table))
        cmd = SqlQueries.insert_queries[self.table].format(context['execution_date'].hour)
        self.log.info(cmd)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(cmd)
        self.log.info('LoadFactOperator ({self.table}): Done.')



