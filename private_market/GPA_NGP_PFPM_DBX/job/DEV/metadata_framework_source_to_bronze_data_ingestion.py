# Import Factory Classes
from CommonUtilityFunctionsFactory import CommonUtilityFunctionsFactory
from MetadataConstants import MetadataConstants
from DataReaderFactory import DataReaderFactory
from DataWriterFactory import DataWriterFactory
from setup_logging import setup_logging
from audit_factory import AuditFactory
from FileFactory import FileFactory
from DateUtilsFactory import DateUtilsFactory
from EnvironmentConfigLoader import EnvironmentConfigLoader


# Get PBR layer defaults using the factory
metadata_defaults = MetadataConstants.as_dict()

# Get merged constants using the config loader factory
const_dict = EnvironmentConfigLoader.get_config(env, metadata_defaults)

# Create one instance for metadata config class
# config = MetadataConstants.get_config(env)

# Defined Data ingestion function to process data from Source to Bronze Layer
def data_ingestion():
    setup_logging()

    spark = CommonUtilityFunctionsFactory.get_active_spark_session(
        "Metadata ingestion Pipeline"
    )

    job_id = CommonUtilityFunctionsFactory.generate_pipeline_ID(
        MetadataConstants.PM_BRONZE_SCHEMA_VAR,
        MetadataConstants.PIPELINE_NAME
    )

    logging.info(f"Pipeline execution started for the Job ID: {job_id}")

    AuditFactory.write_log(
        spark,
        job_id,
        "Job Start",
        "INFO",
        "Pipeline execution started"
    )

    # Create one instance for DataReaderFactory Class
    source_reader = DataReaderFactory.get_reader("unity_catalog")

    
# Read data from entity metadata table
    entity_metadata_row = (
        source_reader.read(
            spark,
            f"{MetadataConstants.PM_SNOWFLAKE_CATALOG_VAR}."
            f"{MetadataConstants.PM_SNOWFLAKE_SCHEMA_VAR}."
            f"{MetadataConstants.TABLE_NAME_ENTITY_METADATA}"
        )
        .filter(F.col("ACTIVE_FLAG") == "Y")
        & (F.col("ENTITY_ID") == var_entity_id)
    ).first()

    # Declare variables from entity metadata
    source_id = entity_metadata_row.SOURCE_ID
    client_name = entity_metadata_row.CLIENT_NAME
    entity_id = var_entity_id
    entity_type = entity_metadata_row.ENTITY_TYPE

# Input in yyyymmdd format
quarter_end_date = datetime.strptime(
    var_quarter_end_date, "%Y%m%d"
).strftime("%Y-%m-%d")

quarter_start_date = DateUtilsFactory.calculate(
    spark,
    var_quarter_end_date
)

# Read Data from Extract Config table to get all source and destination connection details
extract_config_df = source_reader.read(
    spark,
    f"{MetadataConstants.PM_SNOWFLAKE_CATALOG_VAR}."
    f"{MetadataConstants.PM_SNOWFLAKE_SCHEMA_VAR}."
    f"{MetadataConstants.TABLE_NAME_EXTRACT_CONFIG}"
)
