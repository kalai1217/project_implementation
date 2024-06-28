# Databricks notebook source
# MAGIC %md
# MAGIC ##### Mount ADLS Storage

# COMMAND ----------

# container_name = 'vtex_project_db'
# storage_account_name = 'vtexdb'
# dbutils.fs.mount(
#   source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
#   mount_point = '/mnt/vtex',
#   extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
# )

# COMMAND ----------

read_df =spark.read.format('json').load('/mnt/vtex/project_implementation/vtex_test_data.json')
read_df.display()

# COMMAND ----------

read_df.printSchema()