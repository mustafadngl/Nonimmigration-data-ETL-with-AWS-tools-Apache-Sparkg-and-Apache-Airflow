from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 checking_vars =[],
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.checking_vars=checking_vars
        self.tables=tables        
    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for check in self.checking_vars:
            fail_count=0
            fail_case=[]
            export_sql=check.get("check_sql")
            expected_val=check.get("expected_value")
            evaulated_val= redshift.get_record(export_sql)
            if evaulated_val[0]!=expected_val:
                fail_count+=1
                fail_case.append(export_sql+"\n")
        if fail_count>0:
            self.log.info(f"Quality check test failed {fail_case}")
            self.log.info(f"test sql failed {fail_count} for trials")
            raise ValueError ("Data Quality Check Failed!")
                
        for table in self.tables:
            records=redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            first_row=records[0][0]
            if len(records)<1 or len(records[0])<1:
                raise ValueError(f"Data Quality Check failed. No results returned with table {table}.")
            if records[0][0]<(1):
                return ValueError(f"Data Quality Check failed. Table {table} consist 0 rows.")
            else:
                return self.log.info(f"Table {table} passed check quality test with records:"+records[0][0])
              
                