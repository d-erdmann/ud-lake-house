import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705334764150 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lh/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1705334764150",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1705334661164 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-lh/customer_trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1705334661164",
)

# Script generated for node Join
CustomerTrusted_node1705334661164DF = CustomerTrusted_node1705334661164.toDF()
AccelerometerTrusted_node1705334764150DF = AccelerometerTrusted_node1705334764150.toDF()
Join_node1705334740795 = DynamicFrame.fromDF(
    CustomerTrusted_node1705334661164DF.join(
        AccelerometerTrusted_node1705334764150DF,
        (
            CustomerTrusted_node1705334661164DF["email"]
            == AccelerometerTrusted_node1705334764150DF["user"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1705334740795",
)

# Script generated for node SQL Query
SqlQuery915 = """
with ac_count as (
select
user,
count(user) count
from myDataSource
group by user
)

select *
from myDataSource md
join ac_count ac on md.user = ac.user
where count > 1
"""
SQLQuery_node1705334832585 = sparkSqlQuery(
    glueContext,
    query=SqlQuery915,
    mapping={"myDataSource": Join_node1705334740795},
    transformation_ctx="SQLQuery_node1705334832585",
)

# Script generated for node Drop Fields
DropFields_node1705335387318 = DropFields.apply(
    frame=SQLQuery_node1705334832585,
    paths=["x", "y", "user", "z", "timestamp", "count"],
    transformation_ctx="DropFields_node1705335387318",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1705335831507 = DynamicFrame.fromDF(
    DropFields_node1705335387318.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1705335831507",
)

# Script generated for node Customers Curated
CustomersCurated_node1705335158059 = glueContext.getSink(
    path="s3://stedi-lh/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomersCurated_node1705335158059",
)
CustomersCurated_node1705335158059.setCatalogInfo(
    catalogDatabase="stedi-lh", catalogTableName="customer_curated"
)
CustomersCurated_node1705335158059.setFormat("json")
CustomersCurated_node1705335158059.writeFrame(DropDuplicates_node1705335831507)
job.commit()
