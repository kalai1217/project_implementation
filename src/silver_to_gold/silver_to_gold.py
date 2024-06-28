# Databricks notebook source
from pyspark.sql.functions import col, datediff, count, lit, sum, avg, when
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC use vtex_db

# COMMAND ----------

# MAGIC %md
# MAGIC #### Primary Key of Orders Table

# COMMAND ----------

# Create a DataFrame from the Delta table registered in the Hive metastore
orders = spark.read.format("delta").table("vtex_db.orders")

# Show the DataFrame
# orders.display()

result_df = orders.groupBy("order_id").count()
print(result_df.count())

# COMMAND ----------

# Get column names
column_names = orders.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Time taken to fulfill the order (in days). Calculated as the difference between authorizedDate and creationDate.

# COMMAND ----------

# Calculate the time taken to fulfill the order in days
orders1 = orders.withColumn("fulfillment_time", datediff(col("authorized_date"), col("creation_date")))

orders1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Status of the order (e.g., completed, canceled).

# COMMAND ----------

# # Select relevant columns including the status
# gold_customer_order_summary = gold_customer_order_summary.select("order_id", "status")
# gold_customer_order_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Count of orders per status.

# COMMAND ----------

# Define the window specification
window_spec = Window.partitionBy("status")

# Add a column with the count of orders per status
orders2 = orders1.withColumn("order_status_count", count("order_id").over(window_spec))

# Show the resulting DataFrame
orders2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales channel through which the order was placed.

# COMMAND ----------

# Define the window specification
window_spec = Window.partitionBy("sales_channel")

# Add a column with the count of orders per status
orders3 = orders2.withColumn("total_orders_per_channel", count("order_id").over(window_spec))

# Show the resulting DataFrame
orders3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Total sales value per sales channel.

# COMMAND ----------

# Define a window specification over payment_system_name
window_spec = Window.partitionBy("sales_channel")

# Add a new column with the sum of value per payment method using window function
orders4 = orders3.withColumn("total_sales_value_per_channel", sum("value").over(window_spec))

orders4.display()

# COMMAND ----------

# Get column names
column_names = orders4.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Payment  Table

# COMMAND ----------

# Create a DataFrame from the Delta table registered in the Hive metastore
payment = spark.read.format("delta").table("vtex_db.payment")
payment = payment.dropDuplicates()

# Show the DataFrame
payment.display()

# COMMAND ----------

# Get column names
column_names = payment.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Name of the payment method used.

# COMMAND ----------

payment_method_name = payment.select(col("payment_system_name")).distinct()
payment_method_name.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Count of orders per payment method.

# COMMAND ----------

# Define the window specification
window_spec = Window.partitionBy("payment_system_name")

# Count of orders per payment method.
payment1 = payment.withColumn("payment_method_usage_count", count("order_id").over(window_spec))

# Show the resulting DataFrame
payment1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Total value of transactions per payment method.

# COMMAND ----------

# Group by 'payment_system_name' and sum the 'value' column
agg_df = payment1.groupBy("payment_system_name").agg(sum("value").alias("payment_method_total_value"))

# Join the aggregated data back to the original DataFrame
payment2 = payment1.join(agg_df, on="payment_system_name", how="left")

payment2.display()

# COMMAND ----------

# Get column names
column_names = payment2.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Shipping Table

# COMMAND ----------

# Create a DataFrame from the Delta table registered in the Hive metastore
shipping = spark.read.format("delta").table("vtex_db.shipping_data")
shipping = shipping.dropDuplicates()
# Show the DataFrame
shipping.display()

# COMMAND ----------

# Get column names
column_names = shipping.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Average shipping cost

# COMMAND ----------

# Calculate average shipping cost
avg_shipping_cost = shipping.agg(avg("price").alias("avg_shipping_cost")).collect()[0]["avg_shipping_cost"]

# Add average shipping cost as a new column
shipping1 = shipping.withColumn('avg_shipping_cost', lit(avg_shipping_cost))
shipping1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### total_shipping_cost

# COMMAND ----------

# Calculate Total shipping cost
total_shipping_cost = shipping1.selectExpr('sum(price) as total_shipping_cost').collect()[0]['total_shipping_cost']

# Add Total shipping cost as a new column
shipping2 = shipping1.withColumn('total_shipping_cost', lit(total_shipping_cost))
shipping2.display()

# COMMAND ----------

# Get column names
column_names = shipping2.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC #### items table

# COMMAND ----------

# Create a DataFrame from the Delta table registered in the Hive metastore
items = spark.read.format("delta").table("vtex_db.items")
items = items.dropDuplicates()

# Show the DataFrame
items.display()

# COMMAND ----------

# Get column names
column_names = items.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unique identifier for each product.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Total quantity sold for each product.

# COMMAND ----------

# Calculate total quantity sold for each product
total_quantity_sold = items.groupBy("unique_id").agg(sum("items_quantity").alias("total_quantity_sold"))

# Join the calculated total quantity sold back to the original DataFrame
items1 = items.join(total_quantity_sold, "unique_id", "left")

# Show the DataFrame with the new column added
items1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Total sales value for each product.

# COMMAND ----------

from pyspark.sql.functions import col, expr

# Calculate total sales value for each product
items2 = items1.withColumn("total_sales_value", expr("items_quantity * selling_price"))

# Show the DataFrame with the new column added
items2.display()

# COMMAND ----------

# Get column names
column_names = items2.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cancellation Table

# COMMAND ----------

# Create a DataFrame from the Delta table registered in the Hive metastore
cancellation = spark.read.format("delta").table("vtex_db.cancellation")
cancellation = cancellation.dropDuplicates()

# Show the DataFrame
cancellation.display()

# COMMAND ----------

# Get column names
column_names = cancellation.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reason for order cancellation.

# COMMAND ----------

cancellation1 = cancellation.withColumn("cancel_reason", col("reason"))
cancellation1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Count of orders canceled for each reason.

# COMMAND ----------

# Define window specification
windowSpec = Window.partitionBy("reason")

# Add column "cancel_reason_count"
cancellation2 = cancellation1.withColumn("cancel_reason_count", count(when(col("reason").isNotNull(), col("order_id"))).over(windowSpec))

cancellation2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Indicator if the order was returned (1 for yes, 0 for no).

# COMMAND ----------

cancellation3 = cancellation2.withColumn("is_return", when(col("cancellation_date").isNull(), lit(0)).otherwise(lit(1)))
cancellation3.display()

# COMMAND ----------

# Get column names
column_names = cancellation3.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Client Profile Data

# COMMAND ----------

# Create a DataFrame from the Delta table registered in the Hive metastore
client_profile = spark.read.format("delta").table("vtex_db.client_profile")
client_profile = client_profile.dropDuplicates()

# Show the DataFrame
client_profile.display()

# COMMAND ----------

# Get column names
column_names = client_profile.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Email of the customer

# COMMAND ----------

# MAGIC %md
# MAGIC #### Joining Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Count of returns per product

# COMMAND ----------

# Join cancellation and item dataframes on 'order_id' column
cancellation_items = cancellation3.join(items2, 'order_id')

# Group by product identifier and count the number of returns for each product
returns_count_per_product = cancellation_items.groupBy('product_id').count()

returns_count_per_product = returns_count_per_product.withColumnRenamed('count', 'return_count')

# Show the resulting dataframe
# returns_count_per_product.display()

items3 = items2.join(returns_count_per_product,"product_id","left")
items3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Total number of orders placed by each customer.

# COMMAND ----------

# Join the two tables on a common key
joined_df = orders4.join(client_profile, orders4.order_id == client_profile.order_id)

# Group by customer and count the number of orders
orders_per_customer = joined_df.groupBy('user_profile_id').agg(count('client_profile.order_id').alias('total_orders_per_customer'))

orders_per_customer = client_profile.join(orders_per_customer,"user_profile_id","left")

# Show the result
orders_per_customer.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Indicator if the customer is a repeat customer (1 for yes, 0 for no)

# COMMAND ----------

# Create a column indicating if the customer is a repeat customer
orders_per_customer = orders_per_customer.withColumn('is_repeat_customer', when(orders_per_customer.total_orders_per_customer > 1, 1).otherwise(0))

# Include order_id in the final result
total_orders = orders_per_customer.select('order_id', 'total_orders_per_customer', 'is_repeat_customer')

# Show the result
total_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final result

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, avg

# Join tables
joined_df = orders4.join(payment2, "order_id", "full_outer") \
    .join(shipping2, "order_id", "full_outer") \
    .join(items3, "order_id", "full_outer") \
    .join(cancellation3, "order_id", "full_outer") \
    .join(client_profile, "order_id", "full_outer")\
    .join(total_orders,'order_id',"full_outer")


# Show the output DataFrame
joined_df.display()

# COMMAND ----------

# Get column names
column_names = joined_df.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

columns_to_keep = [
    "order_id", "fulfillment_time", "payment_system_name", "payment_method_usage_count",
    "payment_method_total_value", "avg_shipping_cost", "total_shipping_cost", "product_id",
    "total_quantity_sold", "total_sales_value", "cancel_reason", "cancel_reason_count",
    "is_return", "return_count","email", "total_orders_per_customer", "is_repeat_customer",
    "status", "order_status_count", "sales_channel", "total_orders_per_channel",
    "total_sales_value_per_channel"
]

# Select the specified columns
filtered_df = joined_df.select(columns_to_keep)

# Show the result (for verification)
filtered_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Move to Gold Layer

# COMMAND ----------

dbutils.widgets.text("df", "", "df")
df=dbutils.widgets.get("df")

dbutils.widgets.text("path", "", "path")
path=dbutils.widgets.get("path")

dbutils.widgets.text("database_name", "", "database_name")
database_name=dbutils.widgets.get("database_name")

dbutils.widgets.text("target_table_name", "", "target_table_name")
target_table_name=dbutils.widgets.get("target_table_name")

dbutils.widgets.text("merge_col", "", "merge_col")
merge_col=dbutils.widgets.get("merge_col")

df = globals()[df]

# COMMAND ----------

# MAGIC %run /Users/kalaiarasan.j@diggibyte.com/project/bronze_to_silver/common_functions

# COMMAND ----------

# dbutils.fs.rm ('/project_implementation/silver_to_gold/',recurse = True)

# COMMAND ----------

path1 = f'{path}/{target_table_name}'

save_to_delta_with_merge(df, path1, database_name, target_table_name, merge_col)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED vtex_db.gold_customer_order_summary