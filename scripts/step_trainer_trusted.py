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

# Script generated for node Customer Curated
CustomerCurated_node1714175191732 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1714175191732")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1714175152782 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1714175152782")

# Script generated for node SQL Query
SqlQuery461 = '''
select stl.sensorreadingtime, stl.serialnumber, stl.distancefromobject
from 
step_trainer_landing stl
join
customer_curated cc
on
stl.serialnumber = cc.serialnumber
'''
SQLQuery_node1714175536891 = sparkSqlQuery(glueContext, query = SqlQuery461, mapping = {"step_trainer_landing":StepTrainerLanding_node1714175152782, "customer_curated":CustomerCurated_node1714175191732}, transformation_ctx = "SQLQuery_node1714175536891")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1714175766142 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1714175536891, connection_type="s3", format="json", connection_options={"path": "s3://dpsinghvij-bucket/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1714175766142")

job.commit()