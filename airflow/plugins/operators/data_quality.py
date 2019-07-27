from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                  redshift_conn_id = "redshift",
                 sql_commands = "",
                 check_function = "",
                 *args, **kwargs):
        """
            Constructor:
            
            * sql_commands: a dictionnary of scalar queries
            * check_function: a boolean function that maps the the result of the queries to a boolean value. The result True is valued as a success.
            
            """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_commands = sql_commands
        self.check_function = check_function
        logging.info(f"Data quality on test {self.task_id} initialized")


    def execute(self, context): 
        """
            Processor:
        
            Executes a check on the result of the load. Raises an error if the check_function returns False or if the SQL command does not return any record. 
            Adapted from Lesson 2 Exercice 4
        
    """
    
        redshift_hook = PostgresHook("redshift")
        for test_name, sql_command in sql_commands:
            records = redshift_hook.get_records(sql_command)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.test_name} returned no results")
            num_records = records[0][0]
            if not self.check_function(num_records):
                raise ValueError(f"Data quality check failed. {test_name} FAILED: Number of records was: {num_records}")
            logging.info(f"Data quality on test {test_name} passed with {num_records} records")
