MercuryGate TMS ETL Pipeline using Medallion Architecture

This repository contains an ETL pipeline built on the Medallion architecture to ingest and transform freight transportation data from MercuryGate TMS into Azure Data Lake Storage (ADLS).

MercuryGate TMS is a transportation management platform that centralizes and streamlines freight logistics operations. This pipeline is designed to automate the secure extraction, transformation, and loading of MercuryGate data, enabling traceability, quality assurance, and readiness for analytical consumption.

Architecture Overview

The pipeline follows the Medallion (Bronze/Silver/Gold) architecture pattern:

    Bronze Layer: Raw data ingestion for traceability and auditing.

    Silver Layer: Cleaned, transformed, and enriched data ready for analytics and downstream consumption.

    Gold Layer: [Coming soon] Curated data optimized for business intelligence, reporting, and machine learning.

Bronze Layer - Raw Data Ingestion
Step-by-Step Breakdown:

Step 1: Create Temporary Directories

    Creates /tmp/sftp_downloads locally for downloaded files.

    Creates /dbfs/tmp/lau020/sftp_downloads in Databricks File System (DBFS) for intermediate processing.

Step 2: Establish SFTP Connection

    Uses paramiko to connect securely to a remote SFTP server.

    Includes a retry mechanism (up to 3 attempts) in case of failures.

    Confirms successful connection via logging.

Step 3: Download Files in Chunks

    Downloads target files (e.g., claim.txt, claimactivity.txt) from SFTP in 5MB chunks.

    Validates file existence and logs download progress.

    Saves files to /tmp/sftp_downloads.

Step 4: Move Files to DBFS

    Transfers downloaded files to /dbfs/tmp/lau020/sftp_downloads.

Step 5: Load Files into Spark DataFrame

    Reads CSVs from DBFS into Spark with inferred schema.

    Adds audit columns:

        database, year_month, region, country, updated_by, updated_on.

Step 6: Save as Parquet in ADLS

    Determines Parquet storage path in ADLS based on file type.

    Checks for historical presence:

        Writes to historical or raw partitions accordingly.

    Logs destination path.

Step 7: Cleanup Temporary Files

    Deletes files from both local and DBFS temp folders after processing.

Step 8: Close Connections

    Gracefully closes SFTP and SSH connections to prevent resource leakage.

Silver Layer - Clean and Structured Transformation
Process Flow:

Step 1: Define Paths

    Constructs paths for:

        adls_delta_path: Processed data for the current ingestion.

        adls_delta_path_h: Historical data storage.

Step 2: Read from Bronze Layer

    Reads Parquet files from the Bronze Layer.

    If no Silver data exists, initiates a full load; otherwise, continues with updates.

Step 3: Prepare Data

    Converts date columns (e.g., deliverydate) to timestamps if present.

    Adds an active column with default "Y".

    Renames columns for consistency using predefined mappings.

Step 4: Load into Silver Layer

    If no Silver table exists:

        Initializes Delta table at adls_delta_path_h.

        Writes data to both historical and main ingestion paths.

    If Silver table exists:

        Overwrites current Delta table in adls_delta_path with transformed data.

Step 5: Full Load to Silver Layer

    Writes final dataset using Delta format in overwrite mode.

Step 6: Finalize

    Logs ingestion completion per table.

    Tracks total execution time.

    Confirms success or failure.

Future Work - Gold Layer

The Gold Layer will include:

    Aggregated business KPIs

    Data marts optimized for reporting and visualization

    Integration with BI tools such as Power BI and Tableau

Tech Stack

    Apache Spark (PySpark)

    Azure Data Lake Storage (Gen2)

    Databricks

    Delta Lake

    Paramiko (SFTP client)

Folder Structure

soon

Benefits

    Traceability: Raw data stored for auditing and reprocessing.

    Data Quality: Clean, transformed, and standardized records.

    Performance: Optimized writes and reads using Delta Lake.

    Scalability: Built for enterprise-scale data processing on Azure.
