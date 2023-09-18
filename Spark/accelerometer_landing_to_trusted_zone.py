import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometed Landing
AccelerometedLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacityhaint/accelerometer/"], "recurse": True},
    transformation_ctx="AccelerometedLanding_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1695011993450 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityhaint/customer-trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1695011993450",
)

# Script generated for node Join
Join_node1695012035396 = Join.apply(
    frame1=CustomerTrusted_node1695011993450,
    frame2=AccelerometedLanding_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1695012035396",
)

# Script generated for node Filter
Filter_node1695017233102 = Filter.apply(
    frame=Join_node1695012035396,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1695017233102",
)

# Script generated for node Drop Fields
DropFields_node1695013595715 = DropFields.apply(
    frame=Filter_node1695017233102,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1695013595715",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://udacityhaint/accelerometer-trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="dev", catalogTableName="accelerometer-trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1695013595715)
job.commit()
