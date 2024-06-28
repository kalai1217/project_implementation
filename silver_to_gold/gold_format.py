# Databricks notebook source
from pyspark.sql.functions import col, avg, count, when, current_date
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %sql
# MAGIC use vtex_db

# COMMAND ----------

# Create a DataFrame from the Delta table registered in the Hive metastore
orders = spark.read.format("delta").table("vtex_db.orders")

# Show the DataFrame
orders.display()

# COMMAND ----------

# Get column names
column_names = orders.columns

# Print column names
print("Column names:", column_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating items dataframe
# MAGIC

# COMMAND ----------

# Create a DataFrame from the Delta table registered in the Hive metastore
items = spark.read.format("delta").table("vtex_db.items")

# Show the DataFrame
items.display()


# COMMAND ----------

# Get column names
items_names = items.columns

# Print column names
print("Column names:", items_names)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unique identifier for each product

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sum of the quantity sold for each product

# COMMAND ----------

from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window

window_spec = Window.partitionBy("product_id")

# Add the total quantity sold as a new column
items1 = items.withColumn("total_quantity_sold", sum(col("items_quantity")).over(window_spec))

# Display the updated DataFrame
display(items1)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sum of the total sales value for each product
# MAGIC

# COMMAND ----------

window_spec = Window.partitionBy("product_id")

# Add the total sales value as a new column
items2 = items1.withColumn("total_sales_value",sum("selling_price").over(window_spec))

# Display the updated DataFrame
items2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Average price per product
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import avg, col

# Calculate the average price per product
items3 = items2.groupBy("product_id").agg(avg("list_price").alias("average_price"))

# Add the new column to the existing DataFrame
items4 = items2.join(items3, "product_id", "left")

# Display the DataFrame
items4.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Count of total orders
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# Calculate the total orders count
total_orders_count = orders.select("order_id").distinct().count()

# Add a new column with the total orders count to the DataFrame
orders1 = orders.withColumn("total_orders", lit(total_orders_count))

# Display the DataFrame with the new column
orders1.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Count of completed orders

# COMMAND ----------

from pyspark.sql.functions import col, lit

# Filter the DataFrame to include only completed orders
completed_orders_count = orders1.filter(col("is_completed") == "true").count()

# Add a new column with the count of completed orders to the DataFrame
orders2 = orders1.withColumn("completed_orders", lit(completed_orders_count))

# Display the DataFrame with the new column
orders2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Count of canceled orders
# MAGIC

# COMMAND ----------

# Filter the DataFrame to include only canceled orders
cancelled_orders_count = orders2.filter(col("status") == "canceled").count()

# Add a new column with the count of canceled orders to the DataFrame
orders3 = orders2.withColumn("cancelled_orders", lit(cancelled_orders_count))

# Display the DataFrame with the new column
orders3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sum of the value of all orders
# MAGIC

# COMMAND ----------

# Calculate the sum of the value of all orders
total_order_value = orders3.select(sum("value")).collect()[0][0]

# Add a new column with the sum of value of all orders to the DataFrame
orders4 = orders3.withColumn("total_order_value", lit(total_order_value))

# Display the DataFrame with the new column
orders4.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Average value of orders

# COMMAND ----------

from pyspark.sql.functions import avg, lit

# Calculate the average value of the orders
average_order_value = orders4.select(avg("value")).collect()[0][0]

# Add a new column with the average value of orders to the DataFrame
orders5 = orders4.withColumn("average_order_value", lit(average_order_value))

# Display the DataFrame with the new column
orders5.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Percentage of completed orders out of total orders
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit

# Calculate the total number of orders
total_orders_count = orders5.count()

# Calculate the number of completed orders
completed_orders_count = orders5.filter(col("is_completed") == "true").count()

# Calculate the fulfillment rate as a percentage
fulfillment_rate = (completed_orders_count / total_orders_count) * 100

# Add a new column with the fulfillment rate to the DataFrame
orders6 = orders5.withColumn("fulfillment_rate", lit(fulfillment_rate))

# Display the DataFrame with the new column
orders6.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Percentage of canceled orders out of total orders

# COMMAND ----------

# Calculate the total number of orders
total_orders_count = orders6.count()

# Calculate the number of canceled orders
canceled_orders_count = orders6.filter(col("status") == "canceled").count()

# Calculate the cancellation rate as a percentage
cancellation_rate = (canceled_orders_count / total_orders_count) * 100

# Add a new column with the cancellation rate to the DataFrame
orders7 = orders6.withColumn("cancellation_rate", lit(cancellation_rate))

# Display the DataFrame with the new column
orders7.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###client profile df

# COMMAND ----------

# Create a DataFrame from the Delta table registered in the Hive metastore
client_profile = spark.read.format("delta").table("vtex_db.client_profile")

# Show the DataFrame
client_profile.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Unique identifier for each customer (typically email)
# MAGIC

# COMMAND ----------

unique_customers_count = client_profile.select("email").distinct().count()
print(unique_customers_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Average total order value per customer
# MAGIC

# COMMAND ----------

# Join the client profile table with the orders table on "order_id"
joined_df = client_profile.join(orders7, "order_id")

# Calculate the total order value per customer
total_order_value_per_customer = joined_df.groupBy("email").agg(sum("value").alias("total_order_value"))

# Calculate the average total order value per customer
customer_lifetime_value_df = total_order_value_per_customer.select("email", (col("total_order_value")).alias("customer_lifetime_value"))

# Add the "customer_lifetime_value" column to the client profile table
joined1 = client_profile.join(customer_lifetime_value_df, "email", "left")

# Display the DataFrame with the new column
joined1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Count of total orders per customer

# COMMAND ----------

# Calculate the total number of orders per customer
total_orders_per_customer = joined_df.groupBy("email").agg(count("order_id").alias("total_orders_per_customer"))

# Add the "total_orders_per_customer" column to the client profile table
joined2 = joined1.join(total_orders_per_customer, "email", "left")

# Display the DataFrame with the new column
joined2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Average number of items per order per customer

# COMMAND ----------

# Join the client profile table with the orders and items tables on "order_id"
joined_data = client_profile.join(orders7, "order_id").join(items4, "order_id")

# Calculate the total number of items and total number of orders per customer
total_items_and_orders_per_customer = joined_data.groupBy("email").agg(
    sum("items_quantity").alias("total_items"),
    count("order_id").alias("total_orders")
)

# Calculate the average number of items per order per customer
average_items_per_order_df = total_items_and_orders_per_customer.select(
    "email",
    (col("total_items") / col("total_orders")).alias("average_items_per_order")
)

# Add the "average_items_per_order" column to the client profile table
joined3 = joined2.join(average_items_per_order_df, "email", "left")

# Display the DataFrame with the new column
joined3.display()

# COMMAND ----------

# Join tables
joined_table = orders7.join(items4, "order_id", "full_outer") \
    .join(client_profile, "order_id", "full_outer")\
    .join(joined3,"order_id")
# Show the output DataFrame
joined_table.display()

# COMMAND ----------

# Selecting and renaming columns
final_df = joined_table.select(
    col("product_id").alias("product_id"),
    # col("joined_table.client_profile.email").alias("customer_id"),
    col("total_orders").alias("total_orders"),
    col("completed_orders").alias("completed_orders"),
    col("cancelled_orders").alias("canceled_orders"),
    col("total_order_value").alias("total_order_value"),
    col("average_order_value").alias("average_order_value"),
    col("fulfillment_rate").alias("fulfillment_rate"),
    col("cancellation_rate").alias("cancellation_rate"),
    col("customer_lifetime_value").alias("customer_lifetime_value"),
    col("total_orders_per_customer").alias("total_orders_per_customer"),
    col("average_items_per_order").alias("average_items_per_order"),
    col("total_quantity_sold").alias("total_quantity_sold"),
    col("total_sales_value").alias("total_sales_value"),
    col("average_price").alias("average_price"),
    current_date().alias("at_load_date")
)

# Show the final DataFrame
final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Move to gold layer

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

# COMMAND ----------

# MAGIC %run /Users/kalaiarasan.j@diggibyte.com/project/bronze_to_silver/common_functions

# COMMAND ----------

path1 = f'{path}/{target_table_name}'
df = globals()[df]
save_to_delta_with_merge(df, path1, database_name, target_table_name, merge_col)