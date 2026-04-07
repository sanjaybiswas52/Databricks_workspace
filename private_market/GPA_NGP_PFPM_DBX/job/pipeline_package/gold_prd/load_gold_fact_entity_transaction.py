from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import xxhash64, col, current_date, lit, trim, current_timestamp, coalesce
from datetime import datetime
import logging
from pipeline_package.logging.setup_logging import setup_logging
from pipeline_package.utils.CommonUtilityFunctionsFactory import CommonUtilityFunctionsFactory
from pipeline_package.factory.DataReaderFactory import DataReaderFactory
from pipeline_package.factory.DataWriterFactory import DataWriterFactory
from pipeline_package.utils.EnvironmentConfigLoader import EnvironmentConfigLoader
from pipeline_package.utils.GoldPrdConstants import GoldPrdConstants
from pipeline_package.utils.argument_parser import ENVIRONMENT


env = ENVIRONMENT


# Get PRD layer defaults using the factory
PRD_defaults = GoldPrdConstants.as_dict()

# Get merged constants using the config loader factory
const_dict = EnvironmentConfigLoader.get_config(env, PRD_defaults)


# main code
def gold_fact_entity_transaction():

    spark = CommonUtilityFunctionsFactory.get_active_spark_session("Gold fact entity transaction")
    setup_logging()
    source_reader = DataReaderFactory.get_reader("unity_catalog")
    try:
        # Checking the table exist in the catalog
        if CommonUtilityFunctionsFactory.check_tbl_exists(spark, const_dict["pm_medallion_catalog_var"], const_dict["PM_GOLD_SCHEMA_VAR"], const_dict["TABLE_NAME_FACT_ENTITY_TRANSACTION"]):
                                                    #['GOLD_ENTITY_TRANSACTION_TBL']
            # Get data from source table
            source_df = (
                source_reader.read(
                    spark,
                    f"{const_dict['pm_medallion_catalog_var']}."
                    f"{const_dict['PM_GOLD_SCHEMA_VAR']}."
                    f"{const_dict['GOLD_DIM_DATE_TBL']}"
                )
                .alias("dt")
                .join(
                    source_reader.read(
                        spark,
                        f"{const_dict['pm_medallion_catalog_var']}."
                        f"{const_dict['PM_SILVER_SCHEMA_VAR']}."
                        f"{const_dict['GOLD_DIM_DATE_TBL']}"
                    ),
                    col("dt.DATE") == col("gl.GL_DATE_TRANSACTION"),
                    "inner"
                )
                .join(
                    source_reader.read(
                        spark,
                        f"{const_dict['pm_medallion_catalog_var']}."
                        f"{const_dict['PM_SILVER_SCHEMA_VAR']}."
                        f"{const_dict['SILVER_IOS_INVESTMENT_MASTER_TBL']}"
                    ),
                    (col("ivm.SOURCE_ID") == col("gl.SOURCE_ID")) &
                    (col("ivm.CLIENT_ID") == col("gl.CLIENT_ID")) &
                    (col("ivm.ENTITY_ID") == col("gl.ENTITY_ID")),
                    "inner"
                )
                .select(
                    col("gl.ENTITY_ID"),
                    col("gl.CLIENT_ID"),
                    col("gl.SOURCE_ID"),
                    col("gl.TRANSACTION_ID"),
                    col("gl.CURRENCY_CODE"),
                    col("gl.TRANSACTION_AMOUNT_BASE").alias("TRANSACTION_AMOUNT"),
                    col("gl.TRANSACTION_TYPE_DESCRIPTION"),
                    col("gl.TRANSACTION_TYPE_CODE_GENERAL_LEDGER"),
                    col("gl.BATCH_ID_PM"),
                    col("gl.DATE_KEY").alias("TRANSACTIONDATE_KEY"),
                    col("ivm.INVESTOR_NAME").alias("INVESTOR_NAME")

                )
            )
            Source_DF= Source_DF.withColumn("_HASH_ENTITY_ID", xxhash64(col("ENTITY_ID")),trim(col("SOURCE_ID")), trim(col("CLIENT_ID")) )


def gold_fact_entity_transaction():
    try:
        if CommonUtilityFunctionsFactory.check_tbl_exists(
            spark,
            const_dict["pm_medallion_catalog_var"],
            const_dict["PM_GOLD_SCHEMA_VAR"],
            const_dict["GOLD_ENTITY_TRANSACTION_TBL"]
        ):

            parent_id_not_null = (
                source_df
                .select(
                    col("ivm.INVESTOR_NAME").alias("INVESTOR_NAME")
                )
            )

            # union of both the DF
            union_df = parent_id_not_null.unionByName(source_df)

            # Hash Id creation
            hashid_df = (
                union_df
                .withColumn(
                    "_HASH_ENTITY_TRANSACTION_ID",
                    xxhash64(
                        trim(col("ENTITY_ID")),
                        trim(col("INVESTOR_NAME")),
                        trim(col("SOURCE_ID")),
                        trim(col("CLIENT_ID")),
                        trim(col("TRANSACTION_ID"))
                    )
                )
                .withColumn(
                    "_HASH_INVESTOR_ID",
                    xxhash64(
                        trim(col("ENTITY_ID")),
                        trim(col("INVESTOR_NAME")),
                        trim(col("SOURCE_ID")),
                        trim(col("CLIENT_ID"))
                    )
                )
                .withColumn("INSERTED_DATETIME", current_timestamp())
                .withColumn("INSERTED_BY", lit("JOB"))
                .withColumn("UPDATED_DATETIME", current_timestamp())
                .withColumn("UPDATED_BY", lit("JOB"))
                .drop(
                    "SOURCE_ID",
                    "CLIENT_ID",
                    "ENTITY_ID",
                    "INVESTOR_NAME"
                )
            )

            # Insert data into target table
            if not hashid_df.isEmpty():
                target_table = (
                    f"{const_dict['pm_medallion_catalog_var']}."
                    f"{const_dict['PM_GOLD_SCHEMA_VAR']}."
                    f"{const_dict['PRD_FACT_ENTITY_TRANSACTION_TBL']}"
                )

                table_writer = DataWriterFactory.write_data("upsert", spark)
                table_writer.upsert_to_unity_catalog_multi_keys(
                    spark,
                    hashid_df,
                    target_table,
                    const_dict["PRD_FACT_ENTITY_TRANSACTION_TBL"]
                )

                logging.info(
                    f"Data got inserted in "
                    f"{const_dict['GOLD_ENTITY_TRANSACTION_TBL']} table"
                )
            else:
                logging.info(
                    f"No new data to insert in "
                    f"{const_dict['GOLD_ENTITY_TRANSACTION_TBL']} table"
                )

        else:
            logging.info(
                f"Table {const_dict['GOLD_ENTITY_TRANSACTION_TBL']} "
                f"does not exist in gold layer"
            )