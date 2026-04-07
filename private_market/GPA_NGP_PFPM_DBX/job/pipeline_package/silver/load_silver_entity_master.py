from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, cast
from datetime import datetime
import logging

from pipeline.package.utils.PFPM_common_utility_functions import (
    insert_rejected_rows,
    get_null_rows,
    get_duplicate_rows
)
from pipeline.package.logging.setup_logging import setup_logging
from pipeline.package.factory.DataWriterFactory import DataWriterFactory
from pipeline.package.utils.CommonUtilityFunctionsFactory import CommonUtilityFunctionsFactory
from pipeline.package.factory.DataReaderFactory import DataReaderFactory
from pipeline.package.utils.EnvironmentConfigLoader import EnvironmentConfigLoader
from pipeline.package.utils.SilverConstants import SilverConstants
from pipeline.package.utils.argument_parser import ENVIRONMENT


# ---------------------- Define Variables ---------------------- #

env = ENVIRONMENT

# Get silver layer defaults using the factory
silver_defaults = SilverConstants.as_dict()

# Get merged config using the config loader factory
const_dict = EnvironmentConfigLoader.get_config(env, silver_defaults)

# ---------------------- Main Pipeline -------------------------- #

def silver_entity_master():
    spark = CommonUtilityFunctionsFactory.get_active_spark_session(
        "Load Silver Entity Master table"
    )
    setup_logging()
    pipeline_id = None

    try:
        pipeline_id = CommonUtilityFunctionsFactory.generate_pipeline_ID(
            const_dict["PM_SILVER_SCHEMA"]
        )