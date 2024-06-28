# Databricks notebook source
# MAGIC %run ./vtex_schema

# COMMAND ----------

# MAGIC %run ./common_functions

# COMMAND ----------

dbutils.widgets.text("df", "", "df")
df=dbutils.widgets.get("df")
df = globals()[df]

dbutils.widgets.text("path", "", "path")
path=dbutils.widgets.get("path")

dbutils.widgets.text("database_name", "", "database_name")
database_name=dbutils.widgets.get("database_name")

dbutils.widgets.text("target_table_name", "", "target_table_name")
target_table_name=dbutils.widgets.get("target_table_name")

dbutils.widgets.text("merge_col", "", "merge_col")
merge_col=dbutils.widgets.get("merge_col")

# COMMAND ----------

# df.display()

# COMMAND ----------

bronze_df=spark.read.json("/mnt/vtex/project_implementation/vtex_test_data.json")
bronze_df.printSchema()


# COMMAND ----------

raw_df = bronze_df.withColumn("orderDetails", from_json(col("orderDetails"), json_schema))

# COMMAND ----------

final_df = raw_df.select("orderDetails.*")
final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple Parent Columns only

# COMMAND ----------

selected_df = final_df.select("orderId", "sellerOrderId", "origin", "salesChannel", "status", "workflowIsInError", "statusDescription", "value", "creationDate","lastChange","authorizedDate","isCompleted","followUpEmail", "orderGroup")
selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Columns with nested Arrays

# COMMAND ----------

complex_df = final_df.select("orderId","totals","items","clientProfileData","marketingData","ratesAndBenefitsData","shippingData","paymentData","sellers","storePreferencesData","marketplace","itemMetadata","clientPreferencesData","cancellationData")
complex_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploding Complex columns

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. Totals Table

# COMMAND ----------

totals_df_final= final_df.withColumn("totals",explode(final_df["totals"])).select("orderId","totals.*")
totals_df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Items Table

# COMMAND ----------

items_df=final_df.withColumn("items",explode(final_df["items"]))
items_df1=items_df.select("orderId","items.*",items_df["items.name"].alias("items_name"),
        items_df["items.id"].alias("items_id"),items_df["items.price"].alias("items_price"),
        items_df["items.quantity"].alias("items_quantity"))
        
items_df2=items_df1.withColumn("attachments",explode_outer("attachments"))\
                  .withColumn("priceTags", explode_outer("priceTags")) \
                  .withColumn("components", explode_outer("components")) \
                  .withColumn("params", explode_outer("params")) \
                  .withColumn("offerings", explode_outer("offerings")) \
                  .withColumn("attachmentOfferings", explode_outer("attachmentOfferings")) \
                  .withColumn("assemblies", explode_outer("assemblies"))
items_df3=items_df2.select("*",
    "itemAttachment.*",items_df2["itemAttachment.name"].alias("itemAttachment_name"),
    "offerings.*", items_df2["offerings.name"].alias("offerings_name"),
    items_df2["offerings.id"].alias("offerings_id"),
    items_df2["offerings.price"].alias("offerings_price"),"attachmentOfferings.*",
    items_df2["attachmentOfferings.name"].alias("attachmentOfferings_name"),
    "additionalInfo.*",
    "priceDefinition.*").drop("itemAttachment","offerings","attachmentOfferings","additionalInfo","priceDefinition","attachments","components","bundleItems")

items_df4=items_df3.select("*", explode_outer("content").alias("content_key", "content_value")).select("*", explode_outer("schema").alias("schema_key", "schema_value")).drop("content","schema")

items_df5=items_df4.withColumn("categories",explode_outer("categories"))\
                   .withColumn("sellingPrices",explode_outer("sellingPrices"))

items_df6=items_df5.select("*","categories.*",
    items_df5["categories.name"].alias("categories_name"),
    items_df5["categories.id"].alias("categories_id"),"dimension.*","sellingPrices.*",
    items_df5["sellingPrices.quantity"].alias("sellingPrices_quantity"),
"schema_value.*").drop("id","name","price","quantity","categories","dimension","sellingPrices","schema_value")

items_df_final=items_df6.withColumn("Domain",explode_outer("Domain"))
items_df_final.display()

# COMMAND ----------

items_df_final_df = items_df_final.dropDuplicates()
items_df_final_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###3. MarketplaceItems Table

# COMMAND ----------

marketplace_items_df_final = final_df.withColumn("marketplaceItems",explode_outer(final_df["marketplaceItems"])).select("orderId","marketplaceItems")
marketplace_items_df_final.display()

## All Null values, so not considering this table

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. clientProfileData Table

# COMMAND ----------

client_profile_data_df_final = final_df.select("orderId","clientProfileData.*")
client_profile_data_df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###5. ratesAndBenefitsData Table

# COMMAND ----------

ratesAndBenefitsData_df = final_df.select("orderId","ratesAndBenefitsData.*")
ratesAndBenefitsData_df_final=ratesAndBenefitsData_df.withColumn("rateAndBenefitsIdentifiers",explode_outer("rateAndBenefitsIdentifiers"))
ratesAndBenefitsData_df_final1=ratesAndBenefitsData_df_final.select("rateAndBenefitsIdentifiers.*")
# ratesAndBenefitsData_df.display()
# ratesAndBenefitsData_df_final.display()
rates_and_benefits_data_final_df = ratesAndBenefitsData_df_final1.dropna(how='all')
rates_and_benefits_data_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###6. MarketingData Table

# COMMAND ----------

marketingData_df_final = final_df.select("orderId","marketingData.*")
marketing_data_df_final = marketingData_df_final.select("*",explode_outer("marketingTags").alias("marketingTag")).drop("marketingTags")
marketing_data_df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###7. ShippingData Table

# COMMAND ----------

shippingData_df = final_df.select("orderId","shippingData.*")
shippingData_df1=shippingData_df.withColumn('logisticsInfo', explode_outer('logisticsInfo'))
                                
shippingData_df2=shippingData_df1.select("*","logisticsInfo.*",shippingData_df1["logisticsInfo.deliveryChannel"].alias("logistics_deliveryChannel"),
                                         "address.*").drop("logisticsInfo","selectedAddresses.*","address")

shippingData_df3=shippingData_df2.withColumn("slas",explode_outer("slas"))\
                                 .withColumn("shipsTo",explode_outer("shipsTo"))\
                                 .withColumn("deliveryIds",explode_outer("deliveryIds"))\
                                 .withColumn("deliveryChannels",explode_outer("deliveryChannels"))

shippingData_df4=shippingData_df3.select("*", "deliveryIds.*","deliveryChannels.*").drop("slas","deliveryIds","deliveryChannels","pickupStoreInfo")

shipping_data_df_final = shippingData_df4.withColumn("kitItemDetails",explode_outer("kitItemDetails")).drop("selectedAddresses","entityid", "addressid", "id", "versionid")
shipping_data_df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###8. storePreferencesData Table

# COMMAND ----------

storePreferencesData_df =final_df.select("orderId","storePreferencesData.*")
store_preferences_data_df_final = storePreferencesData_df.select("*","currencyFormatInfo.*").drop("currencyFormatInfo")
store_preferences_data_df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###9. paymentsData Table

# COMMAND ----------

paymentData_df = final_df.select("orderId","paymentData.*")
paymentData_df1=paymentData_df.withColumn("giftCards",explode_outer("giftCards"))\
                              .withColumn("transactions",explode_outer("transactions")).select("*","transactions.*").drop("transactions","giftCards")
paymentData_df2=paymentData_df1.withColumn("payments",explode_outer("payments")).select("*","payments.*").drop("payments")
payment_data_df_final=paymentData_df2.select("*",explode_outer("connectorResponses").alias("connector_key","connector_value")).drop("connectorResponses")
payment_data_df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###10. packageAttachmentData Table

# COMMAND ----------

packageAttachment_df = final_df.select("orderId","packageAttachment.*")
package_attachment_df_final = packageAttachment_df.withColumn("packages",explode_outer("packages"))
package_attachment_df_final.display()

## All Null values, so not considering this table

# COMMAND ----------

# MAGIC %md
# MAGIC ###11. sellersData Table

# COMMAND ----------

sellers_df_final = final_df.withColumn("sellers",explode(final_df["sellers"])).select("orderId","sellers.*")
sellers_df_final.display()

# COMMAND ----------

# itemMetadata_df =final_df.select("orderId","itemMetadata.*")
# itemMetadata_df1=itemMetadata_df.withColumn("items",explode_outer("items")).select("*","items.*").drop("items")
# itemMetadata_df2=itemMetadata_df1.withColumn("AssemblyOptions",explode_outer("AssemblyOptions"))
# itemMetadata_df3=itemMetadata_df2.select("*","AssemblyOptions.*",itemMetadata_df2["AssemblyOptions.Id"].alias("Assemble_id"),itemMetadata_df2["AssemblyOptions.Name"].alias("Assembly_namer")).drop("AssemblyOptions","Id","Name")
# itemMetadata_df4=itemMetadata_df3.select("*",explode_outer("InputValues").alias("input_key","input_value")).drop("InputValues")
# itemMetadata_df5=itemMetadata_df4.select("*","input_value.*").drop("input_value")
# itemMetadata_df_final=itemMetadata_df5.withColumn("domain",explode_outer("domain"))
# itemMetadata_df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###12. itemMetaData Table

# COMMAND ----------

# Assuming final_df is your DataFrame with the given schema

# Select the itemMetadata fields and orderId
itemMetadata_df = final_df.select("orderId", "itemMetadata.Items")

# Explode the Items array
itemMetadata_df1 = itemMetadata_df.withColumn("items", explode_outer("Items")).select("*", "items.*").drop("Items", "items")

# Explode the AssemblyOptions array within the exploded items
itemMetadata_df2 = itemMetadata_df1.withColumn("AssemblyOptions", explode_outer("AssemblyOptions"))

# Select all columns and rename AssemblyOptions fields
itemMetadata_df3 = itemMetadata_df2.select("*",
    col("AssemblyOptions.Id").alias("Assemble_id"),
    col("AssemblyOptions.Name").alias("Assembly_name"),
    col("AssemblyOptions.Required"),
    col("AssemblyOptions.InputValues"),
    col("AssemblyOptions.Composition")
).drop("AssemblyOptions")

# Explode the InputValues into two separate columns for 'takeback' and 'garantia-estendida'
itemMetadata_df4 = itemMetadata_df3.select("*",
    col("InputValues.takeback").alias("takeback"),
    col("InputValues.`garantia-estendida`").alias("garantia_estendida")
).drop("InputValues")

# Explode the Domain array in 'takeback'
itemMetadata_df5 = itemMetadata_df4.withColumn("takeback_domain", explode_outer(col("takeback.Domain"))).drop("takeback")

# Explode the Domain array in 'garantia_estendida'
item_meta_data_final = itemMetadata_df5.withColumn("garantia_estendida_domain", explode_outer(col("garantia_estendida.Domain"))).drop("garantia_estendida")

# Display the final DataFrame
item_meta_data_final.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###13. clientPreferencesData Table

# COMMAND ----------

clientpreferences_data_df_final = final_df.select("orderId","clientPreferencesData.*")
clientpreferences_data_df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###14. cancellationData Table

# COMMAND ----------

cancellation_data_df = raw_df.select("orderDetails.orderId","orderDetails.cancellationData.*")
cancellation_data_df = to_snake_case(cancellation_data_df)
cancellation_data_df.display()

# COMMAND ----------

# dbutils.fs.rm ('/project_implementation/bronze_to_silver/',recurse = True)

# COMMAND ----------

# %sql
# create database vtex_db

# COMMAND ----------

# MAGIC %sql
# MAGIC use vtex_db

# COMMAND ----------

path1 = f'{path}/{target_table_name}'

save_to_delta_with_merge(df, path1, database_name, target_table_name, merge_col)