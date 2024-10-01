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

# Script generated for node Customer Trusted
CustomerTrusted_node1727729260612 = glueContext.create_dynamic_frame.from_catalog(database="dianasdb", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1727729260612")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1727729166638 = glueContext.create_dynamic_frame.from_catalog(database="dianasdb", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1727729166638")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1726730118785 = Join.apply(frame1=AccelerometerLanding_node1727729166638, frame2=CustomerTrusted_node1727729260612, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilter_node1726730118785")

# Script generated for node Drop Fields and Duplicates
SqlQuery0 = '''
select distinct customername,email,phone,birthday,
serialnumber,registrationdate,lastupdatedate,
sharewithresearchasofdate,sharewithpublicasofdate,
sharewithfriendsasofdate
from myDataSource
'''
DropFieldsandDuplicates_node1727731711349 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerPrivacyFilter_node1726730118785}, transformation_ctx = "DropFieldsandDuplicates_node1727731711349")

# Script generated for node Customer Curated
CustomerCurated_node1726730189654 = glueContext.getSink(path="s3://lake-house-dianas/project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1726730189654")
CustomerCurated_node1726730189654.setCatalogInfo(catalogDatabase="dianasdb",catalogTableName="customer_curated")
CustomerCurated_node1726730189654.setFormat("json")
CustomerCurated_node1726730189654.writeFrame(DropFieldsandDuplicates_node1727731711349)
job.commit()