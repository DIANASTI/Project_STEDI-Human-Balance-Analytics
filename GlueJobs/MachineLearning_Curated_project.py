import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
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

# Script generated for node StepTrainer Trusted
StepTrainerTrusted_node1727729166638 = glueContext.create_dynamic_frame.from_catalog(database="dianasdb", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1727729166638")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1727776540379 = glueContext.create_dynamic_frame.from_catalog(database="dianasdb", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1727776540379")

# Script generated for node Customer Privacy Filter
StepTrainerTrusted_node1727729166638DF = StepTrainerTrusted_node1727729166638.toDF()
AccelerometerTrusted_node1727776540379DF = AccelerometerTrusted_node1727776540379.toDF()
CustomerPrivacyFilter_node1726730118785 = DynamicFrame.fromDF(StepTrainerTrusted_node1727729166638DF.join(AccelerometerTrusted_node1727776540379DF, (StepTrainerTrusted_node1727729166638DF['sensorreadingtime'] == AccelerometerTrusted_node1727776540379DF['timestamp']), "left"), glueContext, "CustomerPrivacyFilter_node1726730118785")

# Script generated for node Drop Fields and Duplicates
SqlQuery0 = '''
select user ,timeStamp  ,serialNumber
from myDataSource where timestamp is not null
'''
DropFieldsandDuplicates_node1727731711349 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerPrivacyFilter_node1726730118785}, transformation_ctx = "DropFieldsandDuplicates_node1727731711349")

# Script generated for node MachineLearning Curated
MachineLearningCurated_node1726730189654 = glueContext.getSink(path="s3://lake-house-dianas/project/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1726730189654")
MachineLearningCurated_node1726730189654.setCatalogInfo(catalogDatabase="dianasdb",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1726730189654.setFormat("json")
MachineLearningCurated_node1726730189654.writeFrame(DropFieldsandDuplicates_node1727731711349)
job.commit()