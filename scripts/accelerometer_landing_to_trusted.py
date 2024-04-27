import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1714163836614 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1714163836614")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1714163820910 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1714163820910")

# Script generated for node Join
Join_node1714163872265 = Join.apply(frame1=CustomerTrusted_node1714163836614, frame2=AccelerometerLanding_node1714163820910, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1714163872265")

# Script generated for node Drop Fields
DropFields_node1714165373002 = DropFields.apply(frame=Join_node1714163872265, paths=["email", "phone", "sharewithpublicasofdate", "sharewithresearchasofdate", "lastupdatedate", "registrationdate", "serialnumber", "birthdate", "customername"], transformation_ctx="DropFields_node1714165373002")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714164583993 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1714165373002, connection_type="s3", format="json", connection_options={"path": "s3://dpsinghvij-bucket/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AccelerometerTrusted_node1714164583993")

job.commit()