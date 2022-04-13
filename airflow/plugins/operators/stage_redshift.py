from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields=("s3_key",)
    copy_sql=""" 
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS '{}'
        REGION '{}'                      
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 data_format="",       
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id= aws_credentials_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.region=region
        self.data_format=data_format
        
    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials_id)
        credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Cleaning unwanted beforehand data from destination redshift table")
        redshift.run("TRUNCATE {}".format(self.table))
        
        self.log.info("Copy data from S3 bucket to Redshift")
        rendered_key= self.s3_key.format(**context)
        s3_path= "s3://{}/{}".format(self.s3_bucket,rendered_key)
        formatted_sql= StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.data_format,
            self.region)
        redshift.run(formatted_sql)