from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql
        self.table=table
        self.truncate=truncate        

    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info("Truncate table from Redshift in case of protection from load dump to table")
            redshift.run("TRUNCATE {}".format(self.table))
        
        self.log.info("Creating dim table {}".format(self.table))
        redshift.run("INSERT INTO {} {}".format(self.table,self.sql))