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

# Script generated for node Customer Landing
CustomerLanding_node1705331872743 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-lh/customer_landing/"], "recurse": True},
    transformation_ctx="CustomerLanding_node1705331872743",
)

# Script generated for node SQL Query
SqlQuery895 = """
select * from myDataSource where sharewithresearchasofdate is not null

"""
SQLQuery_node1705333305291 = sparkSqlQuery(
    glueContext,
    query=SqlQuery895,
    mapping={"myDataSource": CustomerLanding_node1705331872743},
    transformation_ctx="SQLQuery_node1705333305291",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705332850211 = glueContext.getSink(
    path="s3://stedi-lh/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1705332850211",
)
CustomerTrusted_node1705332850211.setCatalogInfo(
    catalogDatabase="stedi-lh", catalogTableName="customer_trusted"
)
CustomerTrusted_node1705332850211.setFormat("json")
CustomerTrusted_node1705332850211.writeFrame(SQLQuery_node1705333305291)
job.commit()
