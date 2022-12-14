# Databricks notebook source
# MAGIC %md
# MAGIC ## DS-3002: Sample Capstone Project
# MAGIC This notebook demonstrates many of the software libraries and programming techniques required to fulfill the requirements of the final end-of-session capstone project for course **DS-3002: Data Systems** at the University of Virginia School of Data Science. The spirit of the project is to provide a capstone challenge that requires students to demonstrate a practical and functional understanding of each of the data systems and architectural principles covered throughout the session.
# MAGIC 
# MAGIC **These include:**
# MAGIC - Relational Database Management Systems (e.g., MySQL, Microsoft SQL Server, Oracle, IBM DB2)
# MAGIC   - Online Transaction Processing Systems (OLTP): *Relational Databases Optimized for High-Volume Write Operations; Normalized to 3rd Normal Form.*
# MAGIC   - Online Analytical Processing Systems (OLAP): *Relational Databases Optimized for Read/Aggregation Operations; Dimensional Model (i.e, Star Schema)*
# MAGIC - NoSQL *(Not Only SQL)* Systems (e.g., MongoDB, CosmosDB, Cassandra, HBase, Redis)
# MAGIC - File System *(Data Lake)* Source Systems (e.g., AWS S3, Microsoft Azure Data Lake Storage)
# MAGIC   - Various Datafile Formats (e.g., JSON, CSV, Parquet, Text, Binary)
# MAGIC - Massively Parallel Processing *(MPP)* Data Integration Systems (e.g., Apache Spark, Databricks)
# MAGIC - Data Integration Patterns (e.g., Extract-Transform-Load, Extract-Load-Transform, Extract-Load-Transform-Load, Lambda & Kappa Architectures)
# MAGIC 
# MAGIC What's more, this project requires students to make effective decisions regarding whether to implement a Cloud-hosted, on-premises hosted, or hybrid architecture.
# MAGIC 
# MAGIC ### Section I: Prerequisites
# MAGIC 
# MAGIC #### 1.0. Import Required Libraries

# COMMAND ----------

import os
import json
import pymongo
import pyspark.pandas as pd  # This uses Koalas that is included in PySpark version 3.2 or newer.
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Instantiate Global Variables

# COMMAND ----------

# Azure SQL Server Connection Information #####################
jdbc_hostname = "laurenmds2002.mysql.database.azure.com"
jdbc_port = 3306
src_database = "police_killings"

connection_properties = {
  "user" : "lmarkwart",
  "password" : "Langley123",
  "driver" : "org.mariadb.jdbc.Driver"
}

# MongoDB Atlas Connection Information ########################
atlas_cluster_name = "Project 0"
atlas_database_name = "policekillings"
atlas_user_name = "laurenmarkwart"
atlas_password = "Password123"

# Data Files (JSON) Information ###############################
dst_database = "police_killings"

base_dir = "dbfs:/FileStore/ds3002-capstone"
database_dir = f"{base_dir}/{dst_database}"

data_dir = f"{base_dir}/source_data"
batch_dir = f"{data_dir}/batch"
stream_dir = f"{data_dir}/stream"

output_bronze = f"{database_dir}/fact_sales_orders/bronze"
output_silver = f"{database_dir}/fact_sales_orders/silver"
output_gold   = f"{database_dir}/fact_sales_orders/gold"

# Delete the Streaming Files ################################## 
dbutils.fs.rm(f"{database_dir}/fact_sales_orders", True)

# Delete the Database Files ###################################
dbutils.fs.rm(database_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Define Global Functions

# COMMAND ----------

# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the Azure SQL database server.
# ######################################################################################################################
def get_sql_dataframe(host_name, port, db_name, conn_props, sql_query):
    '''Create a JDBC URL to the Azure SQL Database'''
    jdbcUrl = f"jdbc:mysql://{host_name}:{port}/{db_name}"
    
    '''Invoke the spark.read.jdbc() function to query the database, and fill a Pandas DataFrame.'''
    dframe = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=conn_props)
    
    return dframe


# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the MongoDB Atlas database server Using PyMongo.
# ######################################################################################################################
def get_mongo_dataframe(user_id, pwd, cluster_name, db_name, collection, conditions, projection, sort):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.jeczt7v.mongodb.net/{db_name}?retryWrites=true&w=majority"
    
    client = pymongo.MongoClient(mongo_uri)

    '''Query MongoDB, and fill a python list with documents to create a DataFrame'''
    db = client[db_name]
    if conditions and projection and sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection).sort(sort)))
    elif conditions and projection and not sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection)))
    else:
        dframe = pd.DataFrame(list(db[collection].find()))

    client.close()
    
    return dframe

# ######################################################################################################################
# Use this Function to Create New Collections by Uploading JSON file(s) to the MongoDB Atlas server.
# ######################################################################################################################
def set_mongo_collection(user_id, pwd, cluster_name, db_name, src_file_path, json_files):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.jeczt7v.mongodb.net/{db_name}?retryWrites=true&w=majority"
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    
    '''Read in a JSON file, and Use It to Create a New Collection'''
    for file in json_files:
        db.drop_collection(file)
        json_file = os.path.join(src_file_path, json_files[file])
        with open(json_file, 'r') as openfile:
            json_object = json.load(openfile)
            file = db[file]
            result = file.insert_many(json_object)

    client.close()
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section II: Populate Dimensions by Ingesting Reference (Cold-path) Data 
# MAGIC #### 1.0. Fetch Reference Data From an Azure SQL Database
# MAGIC ##### 1.1. Create a New Databricks Metadata Database, and then Create a New Table that Sources its Data from a View in an Azure SQL database.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS police_killings CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS police_killings
# MAGIC COMMENT "Capstone Project Database"
# MAGIC LOCATION "dbfs:/FileStore/ds3002-capstone/police_killings"
# MAGIC WITH DBPROPERTIES (contains_pii = true, purpose = "DS-3002 Capstone Project");

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_victim
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:mysql://laurenmds2002.mysql.database.azure.com:3306/police_killings",
# MAGIC   dbtable "police_killings.victim",
# MAGIC   user "lmarkwart",
# MAGIC   password "Langley123"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE police_killings;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS police_killings.victim
# MAGIC COMMENT "Date Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/ds3002-capstone/police_killings/date"
# MAGIC AS SELECT * FROM view_victim

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM police_killings.victim LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED police_killings.victim;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Create a New Table that Sources its Data from a Table in an Azure SQL database. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_location
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:mysql://laurenmds2002.mysql.database.azure.com:3306/police_killings",
# MAGIC   dbtable "police_killings.location",
# MAGIC   user "lmarkwart",
# MAGIC   password "Langley123"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE police_killings;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS police_killings.location
# MAGIC COMMENT "Date Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/ds3002-capstone/police_killings/location"
# MAGIC AS SELECT * FROM view_location

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM police_killings.location LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED police_killings.location;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Fetch Reference Data from a MongoDB Atlas Database
# MAGIC ##### 2.1. View the Data Files on the Databricks File System

# COMMAND ----------

display(dbutils.fs.ls(batch_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.2. Create a New MongoDB Database, and Load JSON Data Into a New MongoDB Collection
# MAGIC **NOTE:** The following cell **can** be run more than once because the **set_mongo_collection()** function **is** idempotent.

# COMMAND ----------

source_dir = '/dbfs/FileStore/ds3002-capstone/source_data/batch'
json_files = {"population_facts" : 'population_facts.json'}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, atlas_database_name, source_dir, json_files) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.3. Fetch Data from the New MongoDB Collection

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._
# MAGIC 
# MAGIC val df_population_facts = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "policekillings").option("collection", "population_facts").load()
# MAGIC display(df_population_facts)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_population_facts.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.4. Use the Spark DataFrame to Create a New Table in the Databricks (Adventure Works) Metadata Database

# COMMAND ----------

# MAGIC %scala
# MAGIC df_population_facts.write.format("delta").mode("overwrite").saveAsTable("police_killings.population_facts")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED police_killings.population_facts

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.5. Query the New Table in the Databricks Metadata Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM police_killings.population_facts LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Fetch Data from a File System
# MAGIC ##### 3.1. Use PySpark to Read From a CSV File

# COMMAND ----------

cause_csv = f"{batch_dir}/cause.csv"

df_incident_facts = spark.read.format('csv').options(header='true', inferSchema='true').load(cause_csv)
display(df_incident_facts)

# COMMAND ----------

df_incident_facts.printSchema()

# COMMAND ----------

df_incident_facts.write.format("delta").mode("overwrite").saveAsTable("police_killings.incident_facts")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED police_killings.incident_facts;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM police_killings.incident_facts LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verify Dimension Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE police_killings;
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section III: Integrate Reference Data with Real-Time Data
# MAGIC #### 6.0. Use AutoLoader to Process Streaming (Hot Path) Data 
# MAGIC ##### 6.1. Bronze Table: Process 'Raw' JSON Data

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaHints", "Incident_ID INT")
 .option("cloudFiles.schemaHints", "law_enforcement_agency STRING")
 .option("cloudFiles.schemaHints", "cause STRING") 
 .option("cloudFiles.schemaHints", "armed STRING")
 .option("cloudFiles.schemaHints", "location_ID INT")
 .option("cloudFiles.schemaHints", "street_address STRING")
 .option("cloudFiles.schemaHints", "city STRING")
 .option("cloudFiles.schemaHints", "state STRING")
 .option("cloudFiles.schemaHints", "victim_ID STRING")
 .option("cloudFiles.schemaHints", "name STRING")
 .option("cloudFiles.schemaHints", "age STRING")
 .option("cloudFiles.schemaHints", "gender STRING")
 .option("cloudFiles.schemaHints", "race STRING")
 .option("cloudFiles.schemaLocation", output_bronze)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("multiLine", "true")
 .load(stream_dir)
 .createOrReplaceTempView("orders_raw_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Add Metadata for Traceability */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_bronze_tempview AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM orders_raw_tempview
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze_tempview

# COMMAND ----------

(spark.table("orders_bronze_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_bronze}/_checkpoint")
      .outputMode("append")
      .table("fact_orders_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6.2. Silver Table: Include Reference Data

# COMMAND ----------

(spark.readStream
  .table("fact_orders_bronze")
  .createOrReplaceTempView("orders_silver_tempview"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fact_orders_silver_tempview AS (
# MAGIC   SELECT victim.Victim_ID
# MAGIC     , victim.name AS VictimName
# MAGIC     , victim.age AS VictimAge
# MAGIC     , victim.race AS VictimRace
# MAGIC     , population_facts.population_size
# MAGIC     , population_facts.share_white
# MAGIC     , population_facts.share_black
# MAGIC     , population_facts.share_hispanice
# MAGIC     , population_facts.poverty_rate
# MAGIC     , population_facts.unemployment_rate
# MAGIC     , population_facts.college_degree_rate
# MAGIC     , population_facts.median_personal_income
# MAGIC     , incident_facts.law_enforcement_agency
# MAGIC     , incident_facts.armed
# MAGIC     , incident_facts.cause
# MAGIC     , location.location_ID
# MAGIC     , location.street_address
# MAGIC     , location.city
# MAGIC     , location.state
# MAGIC     , t.Incident_ID
# MAGIC     , t.location_ID
# MAGIC     , t.victim_ID
# MAGIC     , t.population_facts_ID
# MAGIC   FROM orders_silver_tempview t
# MAGIC   INNER JOIN police_killings.victim c
# MAGIC   ON t.Incident_ID = c.victim_ID
# MAGIC   INNER JOIN police_killings.location sa
# MAGIC   ON t.population_facts_ID = CAST(sa.location_ID AS BIGINT)

# COMMAND ----------

(spark.table("fact_orders_silver_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_silver}/_checkpoint")
      .outputMode("append")
      .table("fact_orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED police_killings.fact_orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6.4. Gold Table: Perform Aggregations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT victim_ID
# MAGIC   , VictimName
# MAGIC   , VictimAge
# MAGIC   , VictimRace
# MAGIC   , COUNT(victim_ID) AS victimID
# MAGIC FROM police_killings.fact_orders_silver
# MAGIC GROUP BY Victim_ID, VictimAge, VictimRace, VictimName
# MAGIC ORDER BY victimID DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pc.incident_facts
# MAGIC   , os.cause AS CauseOfDeath
# MAGIC   , os.armed
# MAGIC   , pc.poverty_rate
# MAGIC FROM police_killings.fact_orders_silver AS os
# MAGIC INNER JOIN (
# MAGIC   SELECT Incident_ID
# MAGIC   , COUNT(Victim_ID) AS VictimID
# MAGIC   FROM police_killings.fact_orders_silver
# MAGIC   GROUP BY population_facts_ID
# MAGIC ) AS pc
# MAGIC ON pc.population_facts_ID = os.Incident_facts_ID
# MAGIC ORDER BY Population_Facts_ID DESC
