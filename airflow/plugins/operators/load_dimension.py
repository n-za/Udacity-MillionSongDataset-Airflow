from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

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
    
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_flag = kwargs['params']['append_flag']

    def execute(self, context):
        """
            Processor: It inserts into the table given in the 'table' field.
            
        """
        
        self.log.info('LoadDimensionOperator (' + self.table + ')')
        if not self.append_flag:
            cmd = "BEGIN TRUNCATE TABLE public.{}; ".format(self.table) + SqlQueries.insert_queries[self.table] + "; END;"
        else:
            cmd = SqlQueries.insert_queries[self.table] + SqlQueries.not_exists_subqueries[self.table]

        self.log.info(cmd)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(cmd)
        self.log.info('LoadDimensionOperator (' + self.table + '): Done.')


