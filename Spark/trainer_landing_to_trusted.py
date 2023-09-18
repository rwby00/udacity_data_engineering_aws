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

# Script generated for node step_trainer
step_trainer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacityhaint/step_trainer/"], "recurse": True},
    transformation_ctx="step_trainer_node1",
)

# Script generated for node customer_curated
customer_curated_node1695014724284 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityhaint/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1695014724284",
)

# Script generated for node Join
Join_node1695014807983 = Join.apply(
    frame1=step_trainer_node1,
    frame2=customer_curated_node1695014724284,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1695014807983",
)

# Script generated for node Drop Fields
DropFields_node1695014891901 = DropFields.apply(
    frame=Join_node1695014807983,
    paths=[
        "`.serialNumber`",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1695014891901",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://udacityhaint/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="dev", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1695014891901)
job.commit()
