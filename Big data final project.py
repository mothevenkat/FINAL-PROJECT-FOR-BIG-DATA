# Databricks notebook source
# MAGIC %md
# MAGIC ##Loading the data

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, regexp_replace, trim, to_timestamp

CSV_URL = "/Workspace/Users/myadamraghavendra2001@gmail.com/data.csv"

raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .load(CSV_URL)

print(f"Raw DataFrame loaded with {raw_df.count()} records.")

raw_df.printSchema()

# ✅ Use this in Databricks
display(raw_df.limit(5))

# ✅ Use this in normal PySpark (if display() fails)
# raw_df.limit(5).show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Cleaning the data

# COMMAND ----------

df_cleaned = raw_df.filter(~col("InvoiceNo").startswith("C")) \
                     .filter(col("CustomerID").isNotNull())

df_final = df_cleaned.withColumn("Sales", col("Quantity") * col("UnitPrice")) \
                     .withColumn("InvoiceTimestamp", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")) \
                     .select(
                         col("InvoiceNo"),
                         col("StockCode"),
                         trim(col("Description")).alias("Description"), 
                         col("Quantity"),
                         col("InvoiceTimestamp").alias("InvoiceDate"),
                         col("UnitPrice"),
                         col("CustomerID"),
                         col("Country"),
                         col("Sales")
                     )

print(f"Cleaned DataFrame has {df_final.count()} records.")
df_final.printSchema()
display(df_final.limit(5))

DATABASE_NAME = "final_exam_db"
TABLE_NAME = "online_retail_data"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{DATABASE_NAME}.{TABLE_NAME}")

print(f"Data successfully written to Delta table: {DATABASE_NAME}.{TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ##Total sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Description,
# MAGIC   SUM(Sales) AS TotalSales
# MAGIC FROM
# MAGIC   final_exam_db.online_retail_data
# MAGIC GROUP BY
# MAGIC   Description
# MAGIC ORDER BY
# MAGIC   TotalSales DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##Total sales by country

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Country,
# MAGIC   SUM(Sales) AS TotalSales,
# MAGIC   COUNT(DISTINCT CustomerID) AS UniqueCustomers
# MAGIC FROM
# MAGIC   final_exam_db.online_retail_data
# MAGIC GROUP BY
# MAGIC   Country
# MAGIC ORDER BY
# MAGIC   TotalSales DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Sales Revenue and Order Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DATE_FORMAT(InvoiceDate, 'yyyy-MM') AS YearMonth,
# MAGIC   SUM(Sales) AS MonthlySalesRevenue,
# MAGIC   COUNT(DISTINCT InvoiceNo) AS OrderVolume
# MAGIC FROM final_exam_db.online_retail_data
# MAGIC GROUP BY DATE_FORMAT(InvoiceDate, 'yyyy-MM')
# MAGIC ORDER BY YearMonth;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Value Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Country,
# MAGIC   ROUND(SUM(Sales) / COUNT(DISTINCT CustomerID), 2) AS AvgSalesPerCustomer
# MAGIC FROM final_exam_db.online_retail_data
# MAGIC GROUP BY Country
# MAGIC ORDER BY AvgSalesPerCustomer DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Market Activity Level

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Country,
# MAGIC   COUNT(*) AS TotalTransactions
# MAGIC FROM final_exam_db.online_retail_data
# MAGIC GROUP BY Country
# MAGIC ORDER BY TotalTransactions DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Statistics

# COMMAND ----------

retail_df = spark.table("final_exam_db.online_retail_data")
print("Loaded Delta table for analysis.")

print("\nDescriptive Statistics for Quantity, UnitPrice, and Sales:")
retail_df.select('Quantity', 'UnitPrice', 'Sales').describe().show()

from pyspark.sql.functions import sum

top_customers = retail_df.groupBy("CustomerID") \
                         .agg(sum("Sales").alias("TotalSpend")) \
                         .orderBy(col("TotalSpend").desc())

print("\nTop 5 Customers by Total Spend:")
display(top_customers.limit(5))

# COMMAND ----------

retail_df = spark.table("final_exam_db.online_retail_data")
print("Loaded Delta table for analysis.")

print("\nDescriptive Statistics for Quantity, UnitPrice, and Sales:")
retail_df.select('Quantity', 'UnitPrice', 'Sales').describe().show()

from pyspark.sql.functions import sum

top_customers = retail_df.groupBy("CustomerID") \
                         .agg(sum("Sales").alias("TotalSpend")) \
                        .orderBy(col("TotalSpend").desc())

print("\nTop 5 Customers by Total Spend:")
display(top_customers.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sales by specific country

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, regexp_replace, trim, to_timestamp

CSV_URL = "/Workspace/Users/myadamraghavendra2001@gmail.com/data.csv"

raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .load(CSV_URL)

print("Raw DataFrame loaded with {raw_df.count()} records.")
raw_df.printSchema()

display(raw_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ##Country wise quantity

# COMMAND ----------

from pyspark.sql.functions import col, trim, sum

CSV_URL = "/Workspace/Users/myadamraghavendra2001@gmail.com/data.csv"

raw_df = spark.read.csv(
    CSV_URL,
    header=True,
    inferSchema=True
)


df = raw_df.select(
    trim(col("Country")).alias("Country"),
    trim(col("Description")).alias("Description"),
    col("InvoiceNo"),
    col("Quantity"),
    col("StockCode")
)

df.show(5)
print(df.columns)


qty_per_country = df.groupBy("Country") \
    .agg(sum("Quantity").alias("total_quantity"))

display(qty_per_country)





# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Country,
# MAGIC   SUM(Quantity) AS TotalQuantity,
# MAGIC   COUNT(DISTINCT CustomerID) AS UniqueCustomers
# MAGIC FROM
# MAGIC   final_exam_db.online_retail_data
# MAGIC GROUP BY
# MAGIC   Country
# MAGIC ORDER BY
# MAGIC   TotalQuantity DESC;

# COMMAND ----------

from pyspark.sql.functions import col, sum

df = spark.read.format("csv") \
    .option("header", "true") \
    .load("/path/to/your/data.csv")

top_products = df.groupBy("Description") \
    .agg(sum(col("Quantity")).alias("totalquantity")) \
    .orderBy(col("totalquantity").desc()) \
    .limit(10)

print(type(top_products))