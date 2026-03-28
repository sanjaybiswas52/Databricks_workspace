# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date (input_df):
    output = input_df.withColumn("ingestion_date", current_timestamp())
    return output   
    


# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    #partition_column = 'race_id'
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):

  output_df = re_arrange_partition_column(input_df, partition_column)
  """
  #spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  spark.conf.set("spark.sql.partitionOverwriteMode", "dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")
  """

  if f"{db_name}.{table_name}" in [t.name for t in spark.catalog.listTables(db_name)]:
      # Table exists
      #output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
      output_df.write \
        .mode("overwrite") \
        .option("partitionOverwriteMode", "dynamic") \
        .partitionBy(f"{partition_column}") \
        .format("delta") \
        .saveAsTable(f"{db_name}.{table_name}")
  else:
      # Table does not exist
      output_df.write.mode("overwrite") \
          .partitionBy(partition_column) \
          .format("delta") \
          .saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                          .distinct() \
                          .collect()

    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list


# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    #spark.confi.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    from delta.tables import DeltaTable

    if (spark.catalog.tableExists(f"{db_name}.{table_name}")):
        #deltaTable = DeltaTable.forName(spark, "f1_processed.results")
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition \
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    else:
        input_df.write.mode("overwrite") \
            .partitionBy(partition_column) \
            .format("delta") \
            .saveAsTable(f"{db_name}.{table_name}")