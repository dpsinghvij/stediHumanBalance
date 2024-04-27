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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1714176966734 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1714176966734")

# Script generated for node Customers Curated
CustomersCurated_node1714176988441 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomersCurated_node1714176988441")

# Script generated for node accelerometer trusted
accelerometertrusted_node1714177048813 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1714177048813")

# Script generated for node Curation query
SqlQuery565 = '''
select stt.distanceFromObject, at.x, at.y, at.z, at.timestamp, stt.serialnumber
from 
step_trainer_trusted stt
join
customers_curated cc on stt.serialnumber = cc.serialnumber
join
accelerometer_trusted at on stt.sensorreadingtime = at.timestamp
'''
Curationquery_node1714177123357 = sparkSqlQuery(glueContext, query = SqlQuery565, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1714176966734, "customers_curated":CustomersCurated_node1714176988441, "accelerometer_trusted":accelerometertrusted_node1714177048813}, transformation_ctx = "Curationquery_node1714177123357")

# Script generated for node ML Curated Data
MLCuratedData_node1714178946696 = glueContext.write_dynamic_frame.from_options(frame=Curationquery_node1714177123357, connection_type="s3", format="json", connection_options={"path": "s3://dpsinghvij-bucket/ml-curated/", "partitionKeys": []}, transformation_ctx="MLCuratedData_node1714178946696")

job.commit()