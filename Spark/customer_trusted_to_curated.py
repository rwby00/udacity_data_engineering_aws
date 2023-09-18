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

# Script generated for node customer-trusted
customertrusted_node1695014124674 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacityhaint/customer-trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1695014124674",
)

# Script generated for node Accelerometer
Accelerometer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://udacityhaint/accelerometer/"], "recurse": True},
    transformation_ctx="Accelerometer_node1",
)

# Script generated for node Join
Join_node1695014272623 = Join.apply(
    frame1=Accelerometer_node1,
    frame2=customertrusted_node1695014124674,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1695014272623",
)

# Script generated for node Drop Fields
DropFields_node1695014442536 = DropFields.apply(
    frame=Join_node1695014272623,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1695014442536",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://udacityhaint/customer_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="dev", catalogTableName="customers_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1695014442536)
job.commit()
