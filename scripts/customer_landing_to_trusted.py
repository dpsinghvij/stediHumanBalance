import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1714151568055 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dpsinghvij-bucket/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1714151568055")

# Script generated for node SQL Query
SqlQuery560 = '''
select * from customer_landing
where 
shareWithResearchAsOfDate != 0 
and
shareWithResearchAsOfDate is not null
'''
SQLQuery_node1714161352072 = sparkSqlQuery(glueContext, query = SqlQuery560, mapping = {"customer_landing":AmazonS3_node1714151568055}, transformation_ctx = "SQLQuery_node1714161352072")

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1714151867408 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1714161352072, connection_type="s3", format="json", connection_options={"path": "s3://dpsinghvij-bucket/customer/trusted/", "partitionKeys": []}, transformation_ctx="TrustedCustomerZone_node1714151867408")

job.commit()