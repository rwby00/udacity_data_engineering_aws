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

# Script generated for node Amazon S3
AmazonS3_node1695015699976 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityhaint/accelerometer-trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1695015699976",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityhaint/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1695015722141 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1695015699976,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1695015722141",
)

# Script generated for node Drop Fields
DropFields_node1695016212167 = DropFields.apply(
    frame=Join_node1695015722141,
    paths=[],
    transformation_ctx="DropFields_node1695016212167",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://udacityhaint/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="dev", catalogTableName="machine_learning_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1695016212167)
job.commit()
