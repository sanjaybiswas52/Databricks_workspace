## This file implements the same logic in python as the SQL version

from pyspark import pipelines as dp
from pyspark.sql.functions import *


# -------------------------------------------------------------------
# --- 1. Ingest data with autoloader: loop on all folders -----------
# -------------------------------------------------------------------
# Let's loop over all the folders and dynamically generate our SDP pipeline.

#catalog = spark.conf.get("catalog")
catalog = 'retail_catalog'
#schema = spark.conf.get("schema")
schema = 'bronze_db'
table_name = 'customer'

def create_pipeline(table_name):
    print(f"Building SDP CDC pipeline for {table_name}")

    ##Raw CDC Table
    # .option("cloudFiles.maxFilesPerTrigger", "1")
    @dp.table(
        name=table_name + "_cdc",
        comment=f"New {table_name} data incrementally ingested from cloud object storage landing zone",
    )
    def raw_cdc():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"s3://sanjaydatabricks01/json/customers/" + table_name)
        )