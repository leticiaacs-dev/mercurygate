# Databricks notebook source
from pyspark.sql import functions as F
import os
from datetime import datetime, date
from delta.tables import DeltaTable

# COMMAND ----------

column_rename = {    
    'claim': {
    'claimid': 'claim_id',
    'accountid': 'account_id',
    'claimnumber': 'claim_number',
    'claimtype': 'claim_type',
    'statuscodeid':	'status_code_id',
    'statuscode': 'status_code',
    'reasoncodeid':	'reason_code_id',
    'reason': 'reason',
    'claimrep':	'claim_rep',
    'company':	'company',
    'companycode': 'company_code',
    'claimant':	'claim_ant',
    'claimantcode':	'claim_ant_code',
    'claimantline1': 'claim_ant_line1',
    'claimantline2': 'claim_ant_line2',
    'claimantline3': 'claim_ant_line3',
    'contact': 'contact',
    'claimantcontactphone':	'claim_ant_contact_phone',
    'shipper':	'shipper',
    'shippercode':	'shipper_code',
    'shipperline1':	'shipper_line1',
    'shipperline2':	'shipper_line2',
    'shipperline3':	'shipper_line3',
    'deliverydate':	'delivery_date',
    'shipmentdate':	'shipment_date',
    'billladingcarrier': 'bill_lading_carrier',
    'deliverycarrier':	'delivery_carrier',
    'carrierclaimnumber': 'carrier_claim_number',
    'carrierbol': 'carrier_bol',
    'carrierscac':	'carrier_scac',
    'carrier': 'carrier',
    'carriercode':	'carrier_code',
    'carrierline1':	'carrier_line1',
    'carrierline2':	'carrier_line2',
    'carrierline3':	'carrier_line3',
    'customer':	'customer',
    'customercode':	'customer_code',
    'customerline1': 'customer_line1',
    'customerline2': 'customer_line2',
    'customerline3': 'customer_line3',
    'originterminal': 'origin_terminal',
    'originliabilitypct': 'origin_liability_pct',
    'destinationterminal': 'destination_terminal',
    'destinationliabilitypct': 'destination_liability_pct',
    'legalliabilityreserves': 'legal_liability_reserves',
    'transmittalamount': 'transmittal_amount',
    'deniedamount':	'denied_amount',
    'freightamount': 'freight_amount',
    'addlchargesamount': 'addl_charges_amount',
    'totalamount': 'total_amount',
    'paymentamount': 'payment_amount',
    'outstandingamount': 'outstanding_amount',
    'updatedate': 'update_date',
    'datecreated': 'date_created',
    'datefiled': 'date_filed',
    'datemailed': 'date_mailed',
    'dateacknowledged': 'date_acknowledged',
    'dateclosed': 'date_closed',
    'datepaid':	'date_paid',
    'datereopened':	'date_reopened',
    'osdsubmitdate': 'osd_submit_date',
    'datereimburse': 'date_reimburse',
    'comments':	'comments',
    'transportationmode': 'transportation_mode',
    'vehiclenumber': 'vehicle_number',
    'inoutbound': 'in_out_bound',
    'datecancelled': 'date_cancelled',
    'cancelreason':	'cancel_reason',
    'daterejected':	'date_rejected',
    'rejectedreason': 'rejected_reason',
    'datedenied': 'date_denied',
    'denialreason':	'denial_reason',
    'dateapproval':	'date_approval',
    'approvalreason': 'approval_reason',
    'claimgroup': 'claim_group'
    },
    'claim_activities': {
        'rowid': 'row_id',
        'claimnumber': 'claim_number',
        'display': 'display',
        'dateof': 'date_of',
        'accountid': 'account_id',
        'datecreated': 'date_created'
    },
    'claim_additional_charges': {
        'rowid': 'row_id',
        'claimnumber': 'claim_number',
        'chargetype': 'charge_type',
        'description': 'description',
        'amount': 'amount',
        'accountid': 'account_id',
        'datecreated': 'date_created'
    },
    'claim_additional_info': {
        'rowid': 'row_id',
        'claimnumber': 'claim_number',
        'customfield': 'custom_field',
        'value': 'value',
        'accountid': 'account_id',
        'datecreated': 'date_created'
    },
    'claim_diary': {
        'rowid': 'row_id',
        'claimnumber': 'claim_number',
        'dateof': 'date_of',
        'lastupdatedname': 'last_updated_name',
        'category': 'category',
        'comments': 'comments',
        'accountid': 'account_id'
    },
    'claim_documents': {
        'rowid': 'row_id',
        'claimnumber': 'claim_number',
        'display': 'display',
        'value': 'value',
        'dateof': 'date_of',
        'accountid': 'account_id',
        'datecreated': 'date_created'
    },
    'claim_payments': {
        'rowid': 'row_id',
        'claimnumber': 'claim_number',
        'payee': 'payee',
        'payeecode' :'payee_code',
        'payeeline1': 'payee_line1',
        'payeeline2': 'payee_line2',
        'payeeline3': 'payee_line3',
        'paymenttype': 'payment_type',
        'paymentamount': 'payment_amount',
        'paymentdate': 'payment_date',
        'comments': 'comments',
        'requestedby': 'requested_by',
        'requestdate': 'request_date',
        'approvedby': 'approved_by',
        'approvaldate': 'approval_date',
        'checknumber' :'check_number',
        'checkdate': 'check_date',
        'transtype': 'trans_type',
        'transnumber': 'trans_number',
        'transdate' :'trans_date',
        'accountid': 'account_id',
        'datecreated': 'date_created'
    },
    'claim_products': {
        'rowid': 'row_id',
        'claimnumber': 'claim_number',
        'itemnumber': 'item_number',
        'description': 'description',
        'NMFC': 'NMFC',
        'quantity': 'quantity',
        'unitcost': 'unit_cost',
        'weight': 'weight',
        'linetotal': 'line_total',
        'accountid': 'account_id',
        'datecreated': 'date_created'
    }
}

# COMMAND ----------

tms_config = {
    "table_config": {
        "claim": {"primary_key": ["claimnumber"], "orderByCol": ["updated_on"]},
        "claim_activity": {"primary_key": ["claimnumber"], "orderByCol": ["updated_on"]},
        "claim_additionalcharge": {"primary_key": ["claimnumber"], "orderByCol": ["updated_on"]},
        "claim_additionalinfo": {"primary_key": ["claimnumber"], "orderByCol": ["updated_on"]},
        "claim_diary": {"primary_key": ["claimnumber"], "orderByCol": ["updated_on"]},
        "claim_document": {"primary_key": ["claimnumber"], "orderByCol": ["updated_on"]},
        "claim_payment": {"primary_key": ["claimnumber"], "orderByCol": ["updated_on"]},
        "claim_product": {"primary_key": ["claimnumber"], "orderByCol": ["updated_on"]}
    }
}

# Source and Target path
srcPath = "/mnt/fbm-tms-raw/nam/usa/ezclaims/mm"
cleansedPath = "/mnt/fbm-tms-cleansed/nam/usa/ezclaims/mm"

# COMMAND ----------

# DBTITLE 1,Silver layer ingestion
start = datetime.now()
today = date.today()

def process_table(table_name, table_config, src_path, cleansed_path):
    # Paths for Silver Layer
    adls_table_name = table_name
    adls_delta_path = f"{cleansed_path}/{adls_table_name}/datePart={today}"
    adls_delta_path_h = f"{cleansed_path}/{adls_table_name}/datePart=Historic"
    
    # Checks if the table already has data in the Silver Layer
    silver_path = f"{cleansed_path}/{table_name}"
    
    # Reads Bronze layer data
    bronze_df = spark.read.parquet(f"{src_path}/{table_name}")
    
    if not os.path.exists(silver_path):  # If there is no data, perform a full load
        print(f"Starting full load for: {table_name}")
        
        # Checks if Delta table already exists in Historic path
        if not DeltaTable.isDeltaTable(spark, adls_delta_path_h):
            print(f"Directory {adls_delta_path_h} does not exist. Creating Delta table for the first time.")
            # If it does not exist, creates an empty df with the same structure

            # Checks if column 'deliverydate' exists in df
            if 'deliverydate' in bronze_df.columns:
                bronze_df = bronze_df.withColumn("deliverydate", F.to_timestamp(F.col("deliverydate"), "yyyy-MM-dd HH:mm:ss"))
                print("Column 'deliverydate' converted to TimestampType.")
            else:
                print("Column 'deliverydate' not found. No changes made.")

            # Add column 'active' with value 'Y'
            bronze_df = bronze_df.withColumn("active", F.lit("Y"))

            # Rename columns
            for table_rename, columns in column_rename.items():
                for old_col, new_col in columns.items():
                    bronze_df = bronze_df.withColumnRenamed(old_col, new_col)

            # Saves initial data as a Delta table at Historic path
            bronze_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(adls_delta_path_h)
            print(f"Full load for table {table_name} completed. Path: {adls_delta_path_h}")
        else:
            print(f"Delta table already exists in directory: {adls_delta_path_h}. No changes made there.")

        # Perform full load in the main path
        print(f"Starting full load on path: {adls_delta_path}")

        # Rename columns
        for table_rename, columns in column_rename.items():
            for old_col, new_col in columns.items():
                bronze_df = bronze_df.withColumnRenamed(old_col, new_col)

        # Add 'active' column with value 'Y'
        bronze_df = bronze_df.withColumn("active", F.lit("Y"))

        bronze_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(adls_delta_path)
        print(f"Full load performed for {table_name} at path: {adls_delta_path}.")

        print(f"Ingestion of table {table_name} into Silver layer completed successfully!")
        print('-'*100)

    else:
        # If Silver path exists, perform a full load on the main path without altering the historic path
        print(f"Silver layer already exists. Performing full load on {adls_delta_path}.")
        
        # Rename columns
        for table_rename, columns in column_rename.items():
            for old_col, new_col in columns.items():
                bronze_df = bronze_df.withColumnRenamed(old_col, new_col)

        # Add 'active' column with value 'Y'
        bronze_df = bronze_df.withColumn("active", F.lit("Y"))

        # Check if Delta table exists for the main path
        if not os.path.exists(adls_delta_path):  # If the table does not exist, create the Delta table
            print(f"Creating Delta table for {table_name} at path: {adls_delta_path}")
        else:
            print(f"Delta table already exists at path: {adls_delta_path}. Performing full load again.")
        
        # Perform full load (overwrite existing data)
        bronze_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(adls_delta_path)
        print(f"Full load completed for table {table_name} at path: {adls_delta_path}.")
        
        print(f"Ingestion of table {table_name} into Silver layer completed successfully!")
        print('-'*100)


# Process of ingesting configured tables
for table_name, config in tms_config['table_config'].items():
    process_table(table_name, config, srcPath, cleansedPath)

# Captures the end time
end = datetime.now()

# Calculates total execution time
execution_time = end - start
print(f"Total execution time: {execution_time}")