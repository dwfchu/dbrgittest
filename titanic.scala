// Databricks notebook source
// MAGIC %fs ls

// COMMAND ----------

// MAGIC %fs ls dbfs:/FileStore/tables

// COMMAND ----------

val df = spark.read.option("header",true).option("inferSchema",true).csv("dbfs:/FileStore/tables/train_and_test2ws-1.csv")

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

display(df.groupBy("Age").agg(avg("Fare")))

// COMMAND ----------


