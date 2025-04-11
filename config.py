from databricks.sdk.runtime import *
import os
import json

# Dinamically determine the scope to be used

try:
  workspace = spark.conf.get("spark.databricks.workspaceUrl")

  if ".6.azuredatabricks" in workspace:
    secret_scope = "fbmonrampcdtscope"

  else:
    secret_scope = "fbmonrampprdscope"

except:
  secret_scope = "fbmonrampprdscope"

print(secret_scope)

# ----------------------------------------------------------------------------------------------------------------------------------------
# Table configuration

""" Tables
The dictionary below present the tables to be extrated from source to Bronze Layer.

tms_config = {
    "table_config":{
        "($table_name)": {
            "primary_key":["($column_name)", ($column_name)"], 
            "orderByCol":["($column_name)", "($column_name)"]
            }
    }
}
"""
tms_config = {
    "table_config": {
        "claim": {
            "primary_key": ["claimnumber"],
            "orderByCol": [""],
        },
        "claimactivity": {
            "primary_key": ["claimnumber"],
            "orderByCol": [""],
        },
        "claimadditionalcharge": {
            "primary_key": ["claimnumber"],
            "orderByCol": [""],
        },
        "claimadditionalinfo": {
            "primary_key": ["claimnumber"],
            "orderByCol": [""],
        },
        "claimdiary": {
            "primary_key": ["claimnumber"],
            "orderByCol": [""],
        },
        "claimdocument": {
            "primary_key": ["claimnumber"],
            "orderByCol": [""],
        },
        "claimpayment": {
            "primary_key": ["claimnumber"],
            "orderByCol": [""],
        },
        "claimproduct": {
            "primary_key": ["claimnumber"],
            "orderByCol": [""]
        }
        # Add more tables and their configurations here
    }
}

# ----------------------------------------------------------------------------------------------------------------------------------------
# Source to bronze SFTP configuration

credentials = json.loads(dbutils.secrets.get(scope=secret_scope, key='ezclaims-sftp'))

Host=credentials['url']
Port=credentials['port']
Username = credentials['user']
Password = credentials['password']

# ----------------------------------------------------------------------------------------------------------------------------------------
""" Paths
"adls_base_path" stands for the base path in Maestro raw layer. 
That way a folder will be created with the table names to store the data.
"""

# the files download from SFTP to Maestro follow this logic: local file system -> DBFS -> ADLS

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

# ADLS paths
adls_base_path_raw_linehaul = "/mnt/fbm-tms-raw/nam/usa/ezclaims/linehaul/"
basePath_cleansed_linehaul = "/mnt/fbm-tms-cleansed/nam/usa/ezclaims/linehaul/"

adls_base_path_raw_mm = "/mnt/fbm-tms-raw/nam/usa/ezclaims/mm/"
basePath_cleansed_mm = "/mnt/fbm-tms-cleansed/nam/usa/ezclaims/mm/"
