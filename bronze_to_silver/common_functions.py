# Databricks notebook source
from pyspark.sql.functions import udf
import re
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.functions import current_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column renaming function

# COMMAND ----------

def to_snake_case(df: DataFrame) -> DataFrame:
    def convert(name: str) -> str:
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
        return name.lower()

    for column in df.columns:
        snake_case_col = convert(column)
        df = df.withColumnRenamed(column, snake_case_col)
    
    return df
 
udf(to_snake_case)
 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to silver layer as delta table

# COMMAND ----------

def save_to_delta_with_merge(df, path, database_name, target_table_name, merge_col):
    mapped_col = " AND ".join(list(map((lambda x: f"old.{x} = new.{x} "), merge_col))) # if we have multiple PK
    
    df = to_snake_case(df)

    # Add the load_date column to the DataFrame
    df_with_load_date = df.withColumn("load_date", current_date())
    
    if not DeltaTable.isDeltaTable(spark, f"{path}"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        df_with_load_date.write.mode("overwrite").format("delta").option("path", path).saveAsTable(f"{database_name}.{target_table_name}")
    else:
        deltaTable = DeltaTable.forPath(spark, f"{path}")
        deltaTable.alias("old").merge(df_with_load_date.alias("new"), mapped_col)\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()