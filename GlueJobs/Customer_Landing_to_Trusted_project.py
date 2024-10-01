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

# Script generated for node Customer Landing
CustomerLanding_node1726472501967 = glueContext.create_dynamic_frame.from_catalog(database="dianasdb", table_name="accelerometer_landing", transformation_ctx="CustomerLanding_node1726472501967")

# Script generated for node Share with Research
SqlQuery0 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
SharewithResearch_node1726472740980 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1726472501967}, transformation_ctx = "SharewithResearch_node1726472740980")

# Script generated for node Customer Trusted
CustomerTrusted_node1726472935677 = glueContext.getSink(path="s3://lake-house-dianas/project/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1726472935677")
CustomerTrusted_node1726472935677.setCatalogInfo(catalogDatabase="dianasdb",catalogTableName="customer_trusted")
CustomerTrusted_node1726472935677.setFormat("json")
CustomerTrusted_node1726472935677.writeFrame(SharewithResearch_node1726472740980)
job.commit()