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
files = [
    'claim.txt', 'claimactivity.txt', 'claimadditionalcharge.txt',
    'claimadditionalinfo.txt', 'claimdiary.txt', 'claimdocument.txt',
    'claimpayment.txt', 'claimproduct.txt'
]

# Connect to SFTP
client, sftp = connect_to_sftp()

# Function to download files with timeout
def download_with_timeout(sftp, remote_path, local_path, timeout=200):
    def download():
        try:
            sftp.get(remote_path, local_path)
            print(f"Downloaded file: {remote_path} to {local_path}")
        except Exception as e:
            print(f"Error downloading {remote_path}: {e}")

    download_thread = threading.Thread(target=download)
    download_thread.start()
    download_thread.join(timeout)

    if download_thread.is_alive():
        print(f"Download timed out after {timeout} seconds: {remote_path}")
        return False
    return True

# Current date for partitioning
today = date.today()
metadataList = []

# File processing
for file in files:
    sftp_file_path = f'/PILOT/Linehaul/{file}'
    print('-' * 100)
    print(f"Trying to access file at: {sftp_file_path}")

    try:
        sftp.stat(sftp_file_path)
        print(f"File {file} found on SFTP. Starting download...")

        local_file_path = os.path.join(local_temp_path, file)

        if not download_with_timeout(sftp, sftp_file_path, local_file_path, timeout=200):
            print(f"Skipping file {file} due to timeout.")
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

        adls_parquet_path = f"{adls_base_path_raw_linehaul}{adls_table_name}/datePart={today}"
        adls_parquet_path_h = f"{adls_base_path_raw_linehaul}{adls_table_name}/datePart=Historic"

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

        # Measure execution time for each file
        start_time = datetime.now()

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

# Metadata view 
spark.createDataFrame(metadataList).select("table", "database", "count", "execution_time").display()


# COMMAND ----------

# Notify errors
for item in metadataList:
    if item.get("count", 0) == 0:
        raise Exception(f"Error in table {item['table']}: No data extracted.")