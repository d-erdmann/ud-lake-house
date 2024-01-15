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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1705331872743 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lh/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1705331872743",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705341478813 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lh/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1705341478813",
)

# Script generated for node SQL Query
SqlQuery1004 = """
select
*
from step_trainer_trusted s
join accelerometer_trusted a
on s.sensorreadingtime = a.timestamp
"""
SQLQuery_node1705341401166 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1004,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node1705331872743,
        "accelerometer_trusted": AccelerometerTrusted_node1705341478813,
    },
    transformation_ctx="SQLQuery_node1705341401166",
)

# Script generated for node Machine Learning curated
MachineLearningcurated_node1705332850211 = glueContext.getSink(
    path="s3://stedi-lh/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningcurated_node1705332850211",
)
MachineLearningcurated_node1705332850211.setCatalogInfo(
    catalogDatabase="stedi-lh", catalogTableName="machine_learning_curated"
)
MachineLearningcurated_node1705332850211.setFormat("json")
MachineLearningcurated_node1705332850211.writeFrame(SQLQuery_node1705341401166)
job.commit()
