# Get merged constants using the config loader factory
const_dict = EnvironmentConfigLoader.get_config(env, metadata_defaults)

# Defined Data ingestion function to process data from Source to Bronze Layer
def data_ingestion():
    setup_logging()

    spark = CommonUtilityFunctionsFactory.get_active_spark_session(
        "Metadata ingestion Pipeline"
    )

    dbutils = DBUtils(spark)

    sf_options = SnowflakeConnectorFactory.get_snowflake_options(spark)

    job_id = CommonUtilityFunctionsFactory.generate_pipeline_ID(
        const_dict["PM_BRONZE_SCHEMA_VAR"],
        const_dict["PIPELINE_NAME"]
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
    source_reader = DataReaderFactory.get_reader("snowflake")

    # Read data from entity metadata table
    entity_metadata_row = (source_reader.read( spark, sf_options, const_dict["TABLE_NAME_ENTITY_METADATA"] )
        .filter((F.col("ACTIVE_FLAG") == "Y") & (F.col("ENTITY_ID") == var_entity_id))
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
    extract_config_df = (
        source_reader.read( spark, sf_options, const_dict["TABLE_NAME_EXTRACT_CONFIG"])
        .filter((F.col("ACTIVE_FLAG") == "Y") & (F.col("ENTITY_TYPE") == entity_type))
        .filter(
            # These are the file name "GL_INVESTMENT", "GL_INVESTOR" and "ENTITY"
            (F.col("SOURCE_OBJECT").startswith(const_dict["GL_INVESTMENT"])) | 
            (F.col("SOURCE_OBJECT").startswith(const_dict["GL_INVESTOR"])) |
            (F.col("SOURCE_OBJECT").startswith(const_dict["ENTITY"]))
        )
    )

def data_ingestion():

    for meta in extract_config_df.collect():
        try:
            dest_path = meta.SOURCE_CONNECTION + client_name + "/PROCESSED/"

            # Create one instance for FileFactory Class
            fileMover = FileFactory(dbutils, src_path, dest_path)

            source_reader = DataReaderFactory.get_reader(meta.SOURCE_FILE_FORMAT)
            pattern = ( meta.SOURCE_OBJECT + "_"+ var_entity_id + "_"+ var_quarter_end_date )

            # Inputs from your metadata / runtime context
            source_object = (meta.SOURCE_OBJECT or "").upper()
            src_path = src_path  # wherever your files reside
            object_full_name = (
                f"{meta.SOURCE_CONNECTION}{client_name}/"
                f"{meta.SOURCE_OBJECT}.csv"
            )

            # Handler functions encapsulate per-pattern filter logic
            def read_master(object_full_name):
                return (
                    source_reader.read(spark, object_full_name)
                    .filter(F.col("ENTITY_ID") == entity_id)
                )

            def read_gl_investment(object_full_name):
                return (
                    source_reader.read(spark, object_full_name)
                    .filter(F.col("ENTITY_ID") == entity_id)
                    .filter(
                        F.col("GL_DATE_TRANSACTION").between(
                            quarter_start_date, quarter_end_date
                        )
                    )
                )

            def read_gl_investor(object_full_name):
                return (
                    source_reader.read(spark, object_full_name)
                    .filter(F.col("ENTITY_ID") == entity_id)
                    .filter(
                        F.col("EFFECTIVE_DATE").between(
                            quarter_start_date, quarter_end_date
                        )
                    )
                )

            # Return list of object wise pattern definition:
            # (match_objectname_pattern, file_movement_indicator)
            ...

def data_ingestion():

    entity_metadata_row = (
        source_reader.read(
            spark,
            sf_options,
            const_dict["TABLE_NAME_ENTITY_METADATA"]
        )
        .filter(
            (F.col("ACTIVE_FLAG") == "Y") &
            (F.col("ENTITY_ID") == var_entity_id)
        )
        .first()
    )

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
    extract_config_df = (
        source_reader.read(
            spark,
            sf_options,
            const_dict["TABLE_NAME_EXTRACT_CONFIG"]
        )
        .filter(
            (F.col("ACTIVE_FLAG") == "Y") &
            (F.col("ENTITY_TYPE") == entity_type) &
            (
                F.col("SOURCE_OBJECT").startswith(const_dict["GL_INVESTMENT"]) |
                F.col("SOURCE_OBJECT").startswith(const_dict["GL_INVESTOR"]) |
                F.col("SOURCE_OBJECT").startswith(const_dict["ENTITY"])
            )
        )
    )

    # Iterate for individual source connection from Extract Config table
    for meta in extract_config_df.collect():
        try:
            batch_id_pm = str(uuid.uuid4())
            targetTable = meta.TARGET_TABLE
            source_object = meta.SOURCE_OBJECT

            # Variable to define file movement indicator
            # 0 -> No file exists, 1 -> Transaction file exists
            ...