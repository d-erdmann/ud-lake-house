import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1705333757420 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-lh/customer_trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1705333757420",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1705333725217 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lh/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1705333725217",
)

# Script generated for node Join
Join_node1705333785792 = Join.apply(
    frame1=AccelerometerLanding_node1705333725217,
    frame2=CustomerTrusted_node1705333757420,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1705333785792",
)

# Script generated for node Drop Fields
DropFields_node1705334104077 = DropFields.apply(
    frame=Join_node1705333785792,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1705334104077",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1705333843452 = glueContext.getSink(
    path="s3://stedi-lh/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1705333843452",
)
AccelerometerTrusted_node1705333843452.setCatalogInfo(
    catalogDatabase="stedi-lh", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1705333843452.setFormat("json")
AccelerometerTrusted_node1705333843452.writeFrame(DropFields_node1705334104077)
job.commit()
