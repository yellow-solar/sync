""" update DB and then update zoho """

# syslibrary
import os,sys
from datetime import datetime

# third party
import pandas as pd
import numpy as np
import traceback

# custom imports
import tablesync
from tablesync import TableInterface
from yellowpgdb import yellowpgdb
from config import config
from APIconnections import ZohoAPI
from synczoho import zohoSync
from googleapi.gmail import Gmail

# email config
gmail = Gmail('googleservice/mail-93851bb46b8d.json', 'system@yellow.africa')

# YDB table config 
providers = config(section='providers')
core_tables = config(section='solarcore')

TABLES = ['accounts']

### Update the Yellow DB tables
for provider in providers:
# for provider in ['upya']:
    print('------')
    print(provider)
    # for table in TABLES:
    for table in providers[provider].get('tables',[]).keys():
        try:
            tablesync = TableInterface(provider,table)
            tablesync.syncdbtable()
        except Exception as e: 
            # show failure
            print(f"{provider} {table} download and DB sync failed.")
            print('-------------------------------------------------')
            # print traceback
            traceback.print_exc()
            # send an email notifying failure
            gmail.quick_send(
                to = 'ben@yellow.africa, ross@yellow.africa',
                subject = f"DB sync event failed: {provider}.{table}",
                text = f"See AWS log for details <br>{e}",
            )         

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
    for zoho_table in zoho_cfg['sync_tables'].keys():
    # for zoho_table in ['users']:
        print("--------------------------------------")
        print(f"Zoho Import Sync: {zoho_table}")
        zohoSync(zoho_table, provider, zoho)

    # Run the upload sync checker to look for new values
    check = zoho.add("API_Triggers", payload = {"trigger_command":"execute","command_string":"Upload_Sync_Checks"}) 
    if check.status_code==200:
        print("Upload sync checked")
    else: 
        print(check.text)
else:
    print("Can only update Zoho in prod")

print("--------------------------------------")
print("Completed Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))