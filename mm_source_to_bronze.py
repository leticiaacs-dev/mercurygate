# Databricks notebook source
# MAGIC %sh
# MAGIC pip install paramiko

# COMMAND ----------

import paramiko
import os
import threading
import shutil
from datetime import date, datetime
from time import sleep
from pyspark.sql.functions import col, lit, trunc
from config import *

# COMMAND ----------

# DBTITLE 1,download in chunks
# Paramiko can only write to the local file system
local_temp_path = "/tmp/sftp_downloads"
if not os.path.exists(local_temp_path):
    os.makedirs(local_temp_path)
    print(f"Created local temp folder at: {local_temp_path}")

# DBFS
dbfs_temp_path = "/dbfs/tmp/lau020/sftp_downloads"
if not os.path.exists(dbfs_temp_path):
    os.makedirs(dbfs_temp_path)
    print(f"Created DBFS temp folder at: {dbfs_temp_path}")

# Function to connect to SFTP
def connect_to_sftp():
    retries = 0
    while retries < 3:
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(Host, port=Port, username=Username, password=Password, timeout=60, banner_timeout=200)
            transport = client.get_transport()
            transport.set_keepalive(30)
            print("Connected to SFTP")
            return client, client.open_sftp()
        except paramiko.SSHException as e:
            retries += 1
            print(f"Error connecting to SFTP: {e}. Retrying... ({retries}/3)")
            sleep(5)
    raise Exception("Failed to connect to SFTP after 3 retries")

# List of files to download
files = ['claim.txt', 'claimactivity.txt', 'claimadditionalcharge.txt', 'claimadditionalinfo.txt', 'claimdiary.txt', 'claimdocument.txt', 
         'claimpayment.txt', 'claimproduct.txt']

# Connect to SFTP
client, sftp = connect_to_sftp()

# Function to download files in blocks
def download_in_chunks(sftp, remote_path, local_path, chunk_size=5_000_000): # 5mb
    try:
        with sftp.open(remote_path, "rb") as remote_file, open(local_path, "wb") as local_file:
            chunk_count = 0
            total_downloaded = 0
            while True:
                data = remote_file.read(chunk_size)
                if not data:
                    break
                
                chunk_count += 1
                total_downloaded += len(data)
                local_file.write(data)

                # Progress log
                print(f"Chunk {chunk_count} initiated to {remote_path}")
                print(f"Total downloaded so far: {total_downloaded} bytes")

        print(f"Download completed: {remote_path} -> {local_path}")
        return True
    except Exception as e:
        print(f"Error when downloading {remote_path}: {e}")
        return False

# Current date for partitioning
today = date.today()
metadataList = []

local_temp_path = "/tmp/sftp_downloads"

# Check if the directory exists, if not, create it
if not os.path.exists(local_temp_path):
    os.makedirs(local_temp_path)
    print(f"Directory {local_temp_path} created.")

# File processing
for file in files:
    # Measure execution time for each file
    start_time = datetime.now()
    sftp_file_path = f'/PILOT/MM/{file}'
    print('-' * 100)
    print(f"Trying to access file at: {sftp_file_path}")

    try:
        sftp.stat(sftp_file_path)
        print(f"File {file} found on SFTP. Starting download...")

        local_file_path = os.path.join(local_temp_path, file)

        if not download_in_chunks(sftp, sftp_file_path, local_file_path):
            print(f"Skipping file {file} due to download failure.")
            continue

        if os.path.exists(local_file_path):
            file_size = os.path.getsize(local_file_path)
            print(f"File {file} downloaded successfully. Size: {file_size} bytes")
        else:
            print(f"File {file} was not downloaded. Skipping...")
            continue

        # Move to DBFS
        dbfs_file_path = f"{dbfs_temp_path}/{file}"
        dbutils.fs.mv(f"file:{local_file_path}", dbfs_file_path)
        print(f"Moved file {file} to DBFS: {dbfs_file_path}")
        print('-' * 100)

        # Adjust file name for ADLS
        if file == "claim.txt":
            adls_table_name = "claim"
        else:
            adls_table_name = file.replace("claim", "claim_").replace(".txt", "")

        adls_parquet_path = f"{adls_base_path_raw_mm}{adls_table_name}/datePart={today}"
        adls_parquet_path_h = f"{adls_base_path_raw_mm}{adls_table_name}/datePart=Historic"

        # Read CSV file from DBFS
        print(f"Reading file {file} from DBFS...")

        data_df = spark.read.format("csv") \
            .option("header", True) \
            .option("inferSchema", True) \
            .load(f"dbfs:{dbfs_file_path}")

        count = data_df.count()
        print(f"Total records: {count}")

        if count == 0:
            print("No data from the source.")
            continue

        # Adding Audit Columns
        data_df = data_df.withColumn("database", lit("database_name")) \
                         .withColumn("year_month", trunc("datecreated", "month")) \
                         .withColumn("region", lit("NAM")) \
                         .withColumn("country", lit("USA")) \
                         .withColumn("updated_by", lit(dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())) \
                         .withColumn("updated_on", lit(datetime.today()))

        # Check if there is already historical data
        try:
            file_check = len(dbutils.fs.ls(adls_parquet_path_h)) > 0
        except:
            file_check = False

        # Recording in ADLS
        if file_check:
            print("Parquet data is available in the ADLS folder.")
            data_df.write.format("parquet").mode("overwrite").partitionBy("year_month").save(adls_parquet_path)
        else:
            print("No Parquet data is available in the ADLS folder.")
            data_df.write.format("parquet").mode("overwrite").partitionBy("year_month").save(adls_parquet_path_h)

        print(f"Data has been pushed to ADLS bucket path: {adls_parquet_path if file_check else adls_parquet_path_h}")
        print('-' * 100)

        # Add information to metadataList
        metadataList.append({
            "table": adls_table_name,
            "database": "database_name",
            "count": count,
            "execution_time": str(datetime.now() - start_time)
        })

    except FileNotFoundError:
        print(f"File {file} not found on SFTP. Skipping...")
    except Exception as e:
        print(f"Error processing {file}: {e}")

# Closing connections and cleaning directories
sftp.close()
client.close()

# Remove temp files
if os.path.exists(local_temp_path):
    for file in os.listdir(local_temp_path):  
        file_path = os.path.join(local_temp_path, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
    print(f"Cleaned up files inside local temp directory: {local_temp_path}")

# Remove temp files from dbfs        
files = dbutils.fs.ls(dbfs_temp_path)
for file in files:
    dbutils.fs.rm(file.path)  # Remove cada arquivo individualmente
print(f"Cleaned up files inside DBFS temp directory: {dbfs_temp_path}")

# COMMAND ----------

# Exibir metadados no Spark
spark.createDataFrame(metadataList).select("table", "database", "count", "execution_time").display()


# COMMAND ----------

# Notify errors
for item in metadataList:
    if item.get("count", 0) == 0:
        raise Exception(f"Error in table {item['table']}: No data extracted.")