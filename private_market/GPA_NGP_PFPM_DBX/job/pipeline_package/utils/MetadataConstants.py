class MetadataConstants:
    INSERTED_BY = "system_user"
    PM_BRONZE_SCHEMA_VAR = "bronze"
    PM_GOLD_SCHEMA_VAR = "gold"
    PIPELINE_NAME = "Data_Ingestion_From_Source_To_Bronze"
    PM_SNOWFLAKE_SCHEMA_VAR = "consumption_private_market_performance"
    TABLE_NAME_EXTRACT_CONFIG = "EXTRACT_CONFIG"
    TABLE_NAME_ENTITY_METADATA = "ENTITY_METADATA"  #snowflake table
    AUDIT_TABLE_NAME = "audit_log"
    TABLE_NAME_DIM_DATE = "dim_date"
    BRONZE_IOS_ENTITY_MASTER_TBL = "ios_entity_master"
    BRONZE_IOS_GL_ACTIVITY_INVESTMENT_TBL = "ios_general_ledger_activity_investment"
    BRONZE_IOS_GL_ACTIVITY_INVESTOR_TBL = "ios_general_ledger_activity_investor"
    ENTITY = "ENTITY_MASTER"
    GL_INVESTMENT = "GENERAL_LEDGER_ACTIVITY_INVESTMENT"
    GL_INVESTOR = "GENERAL_LEDGER_ACTIVITY_INVESTOR"
    CLIENT_CROSS_REFERENCE = "V_CLIENT_CROSS_REFERENCE"
    SOURCE_SYSTEM_CODE = "MYSS"

    @classmethod
    def as_dict(cls):
        return {
            k: v
            for k, v in cls.__dict__.items()
            if k.isupper() and not k.startswith("__")
        }