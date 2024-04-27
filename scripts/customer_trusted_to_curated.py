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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714172365225 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1714172365225")

# Script generated for node Customer Trusted
CustomerTrusted_node1714172307714 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1714172307714")

# Script generated for node Join Customer and Accelerometer Trusted
SqlQuery501 = '''
select customer_trusted.customername,
customer_trusted.email,
customer_trusted.phone,
customer_trusted.serialnumber,
customer_trusted.registrationdate,
customer_trusted.lastupdatedate,
customer_trusted.sharewithresearchasofdate,
customer_trusted.sharewithpublicasofdate,
customer_trusted.sharewithfriendsasofdate
from 
customer_trusted
join
accelerometer_trusted
on customer_trusted.email = accelerometer_trusted.user
group by customer_trusted.customername, 
customer_trusted.email,
customer_trusted.phone,
customer_trusted.serialnumber,
customer_trusted.registrationdate,
customer_trusted.lastupdatedate,
customer_trusted.sharewithresearchasofdate,
customer_trusted.sharewithpublicasofdate,
customer_trusted.sharewithfriendsasofdate
'''
JoinCustomerandAccelerometerTrusted_node1714172623284 = sparkSqlQuery(glueContext, query = SqlQuery501, mapping = {"customer_trusted":CustomerTrusted_node1714172307714, "accelerometer_trusted":AccelerometerTrusted_node1714172365225}, transformation_ctx = "JoinCustomerandAccelerometerTrusted_node1714172623284")

# Script generated for node Amazon S3
AmazonS3_node1714173116099 = glueContext.write_dynamic_frame.from_options(frame=JoinCustomerandAccelerometerTrusted_node1714172623284, connection_type="s3", format="json", connection_options={"path": "s3://dpsinghvij-bucket/customer/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1714173116099")

job.commit()