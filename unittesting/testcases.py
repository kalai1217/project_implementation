# Databricks notebook source
# MAGIC %run /Users/kalaiarasan.j@diggibyte.com/project/bronze_to_silver/common_functions

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession

# Define the unit test class
class TestToSnakeCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("UnitTest") \
            .getOrCreate()

    def test_to_snake_case(self):
        data = [("kalai", 1), ("arasan", 2)]
        columns = ["FirstName", "LastName"]
        df = self.spark.createDataFrame(data, columns)
        
        expected_columns = ["first_name", "last_name"]
        
        result_df = to_snake_case(df)
        
        self.assertEqual(result_df.columns, expected_columns)

    def test_empty_dataframe(self):
        df = self.spark.createDataFrame([], schema="FirstName STRING, LastName STRING")
        
        expected_columns = ["first_name", "last_name"]
        
        result_df = to_snake_case(df)
        
        self.assertEqual(result_df.columns, expected_columns)

    def test_no_camel_case(self):
        data = [("kalai", 1), ("arasan", 2)]
        columns = ["first_name", "last_name"]
        df = self.spark.createDataFrame(data, columns)
        
        expected_columns = ["first_name", "last_name"]
        
        result_df = to_snake_case(df)
        
        self.assertEqual(result_df.columns, expected_columns)

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date
from delta.tables import DeltaTable

# Define the unit test class
class TestSaveToDeltaWithMerge(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("UnitTest") \
            .getOrCreate()

    def test_save_to_delta_with_merge_new_table(self):
        data = [("kalai", 1), ("arasan", 2)]
        columns = ["FirstName", "LastName"]
        df = self.spark.createDataFrame(data, columns)

        path = "/tmp/delta_table_new"
        database_name = "vtex_db"
        target_table_name = "test_table"
        merge_col = ["FirstName"]
        
        # Cleanup before the test
        self.spark.sql(f"DROP TABLE IF EXISTS {database_name}.{target_table_name}")
        self.spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")
        
        # Ensure the path is clean
        dbutils.fs.rm(path, recurse=True)

        save_to_delta_with_merge(df, path, database_name, target_table_name, merge_col)
        
        # Verify the table is created
        tables = self.spark.catalog.listTables(database_name)
        self.assertTrue(any(table.name == target_table_name for table in tables))

    def test_save_to_delta_with_merge_existing_table(self):
        data = [("kalai", 1), ("arasan", 2)]
        columns = ["FirstName", "LastName"]
        df = self.spark.createDataFrame(data, columns)
        
        path = "/tmp/delta_table_existing"
        database_name = "vtex_db"
        target_table_name = "test_table"
        merge_col = ["FirstName"]
        
        # Cleanup before the test
        self.spark.sql(f"DROP TABLE IF EXISTS {database_name}.{target_table_name}")
        self.spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")
        
        # Ensure the path is clean
        dbutils.fs.rm(path, recurse=True)

        # Create an initial table
        save_to_delta_with_merge(df, path, database_name, target_table_name, merge_col)

        # Modify the data to test the merge
        new_data = [("kalai", 3), ("Ravi", 4)]
        new_df = self.spark.createDataFrame(new_data, columns)
        
        save_to_delta_with_merge(new_df, path, database_name, target_table_name, merge_col)

        # Verify the merge
        result_df = self.spark.table(f"{database_name}.{target_table_name}")
        result_data = result_df.collect()
        
        expected_data = [("kalai", 3), ("arasan", 2), ("Ravi", 4)]
        
        self.assertEqual(sorted(result_data), sorted(expected_data))


if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA vtex_db CASCADE