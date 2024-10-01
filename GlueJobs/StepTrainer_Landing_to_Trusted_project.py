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

# Script generated for node Customer Curated
CustomerCurated_node1727729260612 = glueContext.create_dynamic_frame.from_catalog(database="dianasdb", table_name="customer_curated", transformation_ctx="CustomerCurated_node1727729260612")

# Script generated for node StepTrainer Landing
StepTrainerLanding_node1727729166638 = glueContext.create_dynamic_frame.from_catalog(database="dianasdb", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1727729166638")

# Script generated for node Renamed keys for Customer Privacy Filter
RenamedkeysforCustomerPrivacyFilter_node1727735981388 = ApplyMapping.apply(frame=CustomerCurated_node1727729260612, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("birthday", "string", "right_birthday", "string"), ("registrationdate", "long", "right_registrationdate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("customername", "string", "right_customername", "string"), ("email", "string", "right_email", "string"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("phone", "string", "right_phone", "string"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long")], transformation_ctx="RenamedkeysforCustomerPrivacyFilter_node1727735981388")

# Script generated for node Customer Privacy Filter
StepTrainerLanding_node1727729166638DF = StepTrainerLanding_node1727729166638.toDF()
RenamedkeysforCustomerPrivacyFilter_node1727735981388DF = RenamedkeysforCustomerPrivacyFilter_node1727735981388.toDF()
CustomerPrivacyFilter_node1726730118785 = DynamicFrame.fromDF(StepTrainerLanding_node1727729166638DF.join(RenamedkeysforCustomerPrivacyFilter_node1727735981388DF, (StepTrainerLanding_node1727729166638DF['serialnumber'] == RenamedkeysforCustomerPrivacyFilter_node1727735981388DF['right_serialnumber']), "right"), glueContext, "CustomerPrivacyFilter_node1726730118785")

# Script generated for node Drop Fields and Duplicates
SqlQuery0 = '''
select distinct sensorReadingTime,serialNumber,distanceFromObject
from myDataSource where serialNumber  is not null
'''
DropFieldsandDuplicates_node1727731711349 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerPrivacyFilter_node1726730118785}, transformation_ctx = "DropFieldsandDuplicates_node1727731711349")

# Script generated for node StepTrainer Trusted
StepTrainerTrusted_node1726730189654 = glueContext.getSink(path="s3://lake-house-dianas/project/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1726730189654")
StepTrainerTrusted_node1726730189654.setCatalogInfo(catalogDatabase="dianasdb",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1726730189654.setFormat("json")
StepTrainerTrusted_node1726730189654.writeFrame(DropFieldsandDuplicates_node1727731711349)
job.commit()