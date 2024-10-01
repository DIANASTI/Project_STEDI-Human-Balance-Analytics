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
CustomerTrusted_node1727729260612 = glueContext.create_dynamic_frame.from_catalog(database="dianasdb", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1727729260612")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1727729166638 = glueContext.create_dynamic_frame.from_catalog(database="dianasdb", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1727729166638")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1726730118785 = Join.apply(frame1=AccelerometerLanding_node1727729166638, frame2=CustomerTrusted_node1727729260612, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilter_node1726730118785")

# Script generated for node Drop Fields
DropFields_node1727717505680 = DropFields.apply(frame=CustomerPrivacyFilter_node1726730118785, paths=["email", "phone", "customername", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1727717505680")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1726730189654 = glueContext.getSink(path="s3://lake-house-dianas/project/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1726730189654")
AccelerometerTrusted_node1726730189654.setCatalogInfo(catalogDatabase="dianasdb",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1726730189654.setFormat("json")
AccelerometerTrusted_node1726730189654.writeFrame(DropFields_node1727717505680)
job.commit()