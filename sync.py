""" update DB and then update zoho """

# syslibrary
import os,sys
import datetime 

# third party
import pandas as pd
import numpy as np

# custom imports
import tablesync
from tablesync import TableInterface
from yellowpgdb import yellowpgdb
from config import config
from APIconnections import ZohoAPI
from synczoho import zohoSync

# YDB table config 
providers = config(section='providers')
core_tables = config(section='solarcore')

### Update the tables
TABLES = ['stock']

for provider in providers:
# for provider in ['upya']:
    print('------')
    print(provider)
    for table in TABLES:
    # for table in providers[provider].get('tables',[]).keys():
        # for table in ['stock']:
        tablesync = TableInterface(provider,table)
        tablesync.syncdbtable()
        # tablesync.internalSync()
        
### Run the custom mapping
print("--------------------------------------")
print("Running the core update sql script....")
db = yellowpgdb()
conn = db.connect()
with conn.cursor() as cursor:
    cursor.execute(open("core_update.sql", "r").read())
# commit and close
conn.commit()
conn.close()
print("Successfully updated core tables.")
print("--------------------------------------")

### update zoho
# Fetch zoho cfg and setup API connection object
zoho_cfg = config(section='zoho')
zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])

# loop through each table configured for zoho release
env = config('env')
if env == 'prod':            
    # for zoho_table in zoho_cfg['sync_tables'].keys():
    for zoho_table in ['stock']:
        print(f"Zoho Import Sync: {zoho_table}")
        zohoSync(zoho_table, provider, zoho)
else:
    print("Can only update Zoho in prod")