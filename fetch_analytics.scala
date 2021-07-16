// Databricks notebook source
// 1 => Download the 2 datasets by clicking the links below.
// 2 => Come up with 3 analytics ideas that you'd like to implement
// 3 => Write a scala/java (not pyspark) spark application that can be run with parameters to execute 1 of the 3 analytics ideas where the parameters      indicate:
//              (1) Which analysis to run
//              (2) How many months back to run the analysis
//              (3) Whether to output the results as csv, json, or parquet
// 4 => Write one of the analytical idea implementations using spark SQL.
// 5 => Please share a public github repo with us.
// 6 => The repo should have a readme with instructions on how to get the project running locally.
// 7 => Please assume that the user does not know anything about your chosen technologies
// EX. => Example analysis: Show how many cats landed on their feet in the last 3 months and output the results in json format

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import spark.implicits._

// COMMAND ----------

// Read in Reward Receipts file
val rewardReceipts = spark.read
                          .option("inferSchema","true")
                          .option("header", "true")
                          .format("csv").load("dbfs:/FileStore/shared_uploads/fordjourkyei@yahoo.com/rewards_receipts.csv")

// COMMAND ----------

// Read in first 5 records in Reward Receipts file
rewardReceipts.show(5)

// COMMAND ----------

// Check the schema of the Rewards Receipts csv file
rewardReceipts.printSchema()

// COMMAND ----------

// Count total records on file
rewardReceipts.count()

// COMMAND ----------

//Read in Receipt items csv file
val receiptsItems = spark.read
                          .option("inferSchema","true")
                          .option("header", "true")
                          .format("csv").load("dbfs:/FileStore/shared_uploads/fordjourkyei@yahoo.com/rewards_receipts_item.csv")

// COMMAND ----------

// Read in first 5 records in Reward Receipts file
receiptsItems.show(5)

// COMMAND ----------

//Check the schema of Receipts Items csv file
receiptsItems.printSchema()

// COMMAND ----------

// Count total records on file
receiptsItems.count()

// COMMAND ----------

// Checking unique PRIMARY KEY in receipts file
rewardReceipts.select("RECEIPT_ID").show(5)

// COMMAND ----------

// Checking unique PRIMARY KEY in receipts items file
receiptsItems.select("REWARDS_RECEIPT_ID").show(5)

// COMMAND ----------

// Join both datasets to get a complete picture of the datasets
val joinExpr = rewardReceipts.col("RECEIPT_ID") === receiptsItems.col("REWARDS_RECEIPT_ID")
val receiptsAndItemsDF = rewardReceipts.join(receiptsItems, joinExpr, "inner")

// COMMAND ----------

receiptsAndItemsDF.count() // Check total records after joining both datasets

// COMMAND ----------

receiptsAndItemsDF.printSchema() //Check schema == fields/columns

// COMMAND ----------

// USING SPARK DATAFRAME API - Scala/Spark

// COMMAND ----------

def autoReportsAPI(reportType: String, startDate: String, endDate: String,
                outputFormat: String, outFileName: String,
                df: DataFrame = receiptsAndItemsDF) = {
  val pathName = "dbfs:/FileStore/shared_uploads/fordjourkyei@yahoo.com"
  val (brandSales, salesByState, salesByRewards) = ("BRAND", "STORE_STATE", "STORE_NAME")
  if (reportType == s"$brandSales"){
    df.withColumn("PURCHASE_DATE", 'RECEIPT_PURCHASE_DATE.substr(1,7))
      .where(expr("PURCHASE_DATE").between(s"$startDate",s"$endDate"))
      .select(s"$brandSales", "PURCHASE_DATE", "ITEM_PRICE")
      .groupBy(s"$brandSales").pivot("PURCHASE_DATE")
      .agg(round(sum("ITEM_PRICE"),2).as("Sales"))
      .orderBy(desc(s"$brandSales"))
      .na.fill(0)
      .coalesce(1).write.mode("overwrite").format(s"$outputFormat").option("header", "true")
      .save(s"$pathName/$outFileName.${outputFormat}")
  }else if (reportType == s"$salesByState"){
    df.withColumn("PURCHASE_DATE", 'RECEIPT_PURCHASE_DATE.substr(1,7))
      .where(expr("PURCHASE_DATE").between(s"$startDate",s"$endDate"))
      .select(s"$salesByState", "PURCHASE_DATE", "ITEM_PRICE")
      .groupBy(s"$salesByState").pivot("PURCHASE_DATE")
      .agg(round(sum("ITEM_PRICE"),2).as("Sales"))
      .orderBy(desc(s"$salesByState"))
      .na.fill(0)
      .coalesce(1).write.mode("overwrite").format(s"$outputFormat").option("header", "true")
      .save(s"$pathName/$outFileName.${outputFormat}")
  }else if (reportType == s"$salesByRewards"){
    df.withColumn("PURCHASE_DATE", 'RECEIPT_PURCHASE_DATE.substr(1,7))
      .where(expr("PURCHASE_DATE").between(s"$startDate",s"$endDate"))
      .select(s"$salesByRewards", "PURCHASE_DATE", "ITEM_PRICE")
      .groupBy(s"$salesByRewards").pivot("PURCHASE_DATE")
      .agg(round(sum("ITEM_PRICE"),2).as("Sales"))
      .orderBy(desc(s"$salesByRewards"))
      .na.fill(0)
      .coalesce(1).write.mode("overwrite").format(s"$outputFormat").option("header", "true")
      .save(s"$pathName/$outFileName.${outputFormat})
  }
}  

// COMMAND ----------

//Test outFile to check summary output
spark.read.format("csv").option("inferSchema","true")
          .option("header", "true").load("dbfs:/FileStore/shared_uploads/fordjourkyei@yahoo.com/brand_sales_summary.csv").show(10)

// COMMAND ----------

// Sales Summary By BRAND
autoReportsAPI("BRAND", "2020-01", "2020-03", "csv", "brand_sales_summary")
autoReportsAPI("BRAND", "2020-01", "2020-03", "json", "brand_sales_summary")
autoReportsAPI("BRAND", "2020-01", "2020-03", "parquet", "brand_sales_summary")

// COMMAND ----------

// Sales Summary By STORE_STATE
autoReportsAPI("STORE_STATE", "2020-01", "2020-03", "csv", "store_sales_by_state_summary")
autoReportsAPI("STORE_STATE", "2020-01", "2020-03", "json", "store_sales_by_state_summary")
autoReportsAPI("STORE_STATE", "2020-01", "2020-03", "parquet", "store_sales_by_state_summary")

// COMMAND ----------

// Sales Summary By STORE_STATE
autoReportsAPI("STORE_NAME", "2020-01", "2020-03", "csv", "store_sales_summary")
autoReportsAPI("STORE_NAME", "2020-01", "2020-03", "json", "store_sales_summary")
autoReportsAPI("STORE_NAME", "2020-01", "2020-03", "parquet", "store_sales_summary")

// COMMAND ----------

// USING SPARK SQL - Scala/Spark

// COMMAND ----------

def autoReportsSQL(reportType: String, startDate: String, endDate: String,
                   outputFormat: String, outFileName: String,
                   df: DataFrame = receiptsAndItemsDF) = {
  
  val pathName = "dbfs:/FileStore/shared_uploads/fordjourkyei@yahoo.com"
  val (brandSales, salesByState, salesByRewards) = ("BRAND", "STORE_STATE", "STORE_NAME")
  
  df.createOrReplaceTempView("Receipts_and_items_data")
  if (reportType == s"$brandSales"){
    spark.sql(s"""select $brandSales, substr(RECEIPT_PURCHASE_DATE, 1, 7) as PURCHASE_DATE, round(sum(ITEM_PRICE), 2) as Sales
                  from Receipts_and_items_data
                  where substr(RECEIPT_PURCHASE_DATE, 1, 7) between $startDate and $endDate
                  group by $brandSales, substr(RECEIPT_PURCHASE_DATE, 1, 7)
                  order by $brandSales desc
               """)
          .na.fill(0)
          .coalesce(1).write.mode("overwrite").format(s"$outputFormat").option("header", "true")
          .save(s"$pathName/$outFileName.${outputFormat}")
  }else if (reportType == s"$salesByState"){
    spark.sql(s"""select $salesByState, substr(RECEIPT_PURCHASE_DATE, 1, 7) as PURCHASE_DATE, round(sum(ITEM_PRICE), 2) as Sales
                  from Receipts_and_items_data
                  where substr(RECEIPT_PURCHASE_DATE, 1, 7) between $startDate and $endDate
                  group by $salesByState, substr(RECEIPT_PURCHASE_DATE, 1, 7)
                  order by $salesByState desc
               """)
          .na.fill(0)
          .coalesce(1).write.mode("overwrite").format(s"$outputFormat").option("header", "true")
          .save(s"$pathName/$outFileName.${outputFormat}")
  }else if (reportType == s"$salesByRewards"){
    spark.sql(s"""select $salesByRewards, substr(RECEIPT_PURCHASE_DATE, 1, 7) as PURCHASE_DATE, round(sum(ITEM_PRICE), 2) as Sales
                  from Receipts_and_items_data
                  where substr(RECEIPT_PURCHASE_DATE, 1, 7) between $startDate and $endDate
                  group by $salesByRewards, substr(RECEIPT_PURCHASE_DATE, 1, 7)
                  order by $salesByRewards desc
               """)
          .na.fill(0)
          .coalesce(1).write.mode("overwrite").format(s"$outputFormat").option("header", "true")
          .save(s"$pathName/$outFileName.${outputFormat}")
  }
}  

// COMMAND ----------

// Sales Summary By BRAND
autoReportsSQL("BRAND", "2020-01", "2020-03", "csv", "brand_sales_summary")
autoReportsSQL("BRAND", "2020-01", "2020-03", "json", "brand_sales_summary")
autoReportsSQL("BRAND", "2020-01", "2020-03", "parquet", "brand_sales_summary")

// Sales Summary By STORE_STATE
autoReportsSQL("STORE_STATE", "2020-01", "2020-03", "csv", "store_sales_by_state_summary")
autoReportsSQL("STORE_STATE", "2020-01", "2020-03", "json", "store_sales_by_state_summary")
autoReportsSQL("STORE_STATE", "2020-01", "2020-03", "parquet", "store_sales_by_state_summary")

// Sales Summary By STORE_STATE
autoReportsSQL("STORE_NAME", "2020-01", "2020-03", "csv", "store_sales_summary")
autoReportsSQL("STORE_NAME", "2020-01", "2020-03", "json", "store_sales_summary")
autoReportsSQL("STORE_NAME", "2020-01", "2020-03", "parquet", "store_sales_summary")
