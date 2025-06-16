import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define column names manually (no headers in TSV)
drug_columns = ["stitch_id", "drug_name"]
freq_columns = [
    "stitch_id",        # drug ID
    "umls_id",          # mapped UMLS ID
    "side_effect",      # effect ID
    "placebo",          # usually null or empty
    "freq_percent",     # e.g., 21%
    "frequency",        # numeric value
    "placebo_freq",     # numeric value
    "type",             # PT, LLT, etc.
    "meddra_code",      # code
    "side_effect_name"  # readable label
]

# Load drug_names.tsv
drug_df = spark.read.option("delimiter", "\t").option("header", False).csv("s3://mounika-mlpipeline/drug_names.tsv")
drug_df = drug_df.toDF(*drug_columns)

# Load meddra_freq.tsv
freq_df = spark.read.option("delimiter", "\t").option("header", False).csv("s3://mounika-mlpipeline/meddra_freq.tsv")
freq_df = freq_df.toDF(*freq_columns)

# Join on stitch_id
merged_df = drug_df.join(freq_df, on="stitch_id", how="inner")

# Optional: select only necessary columns
final_df = merged_df.select(
    "stitch_id", "drug_name", "side_effect_name", "frequency"
)

# Save output as CSV with headers
final_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("s3://mounika-mlpipeline/output/merged_tsv/")

# Commit job
job.commit()
