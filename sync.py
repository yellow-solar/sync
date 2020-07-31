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


# YDB table config 
providers_config = config(section='providers')
core_tables = config(section='solarcore')

# Fetch zoho cfg and setup API connection object
zoho_cfg = config(section='zoho')
zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])

# email config
gmail = Gmail('googleservice/mail-93851bb46b8d.json', 'system@yellow.africa')

# set tables
TABLES = []
ZOHO_TABLES = zoho_cfg['sync_tables'].keys()
PROVIDERS = providers_config.keys()

# if more than one argument,# the first is table name
if len(sys.argv) > 1:
    
    if sys.argv[1] in core_tables.keys():
        TABLE_ARG = True
        TABLES = [sys.argv[1]]
        # set zoho tables - not all tables have direct zoho ones though so check first
        if TABLES[0] in ZOHO_TABLES:
            ZOHO_TABLES = TABLES
        else:
            ZOHO_TABLES = []
    
    # if 2 extra arguments,set providers
    if len(sys.argv) == 3:
        if sys.argv[2] in providers_config.keys():
            TABLE_ARG = True
            PROVIDERS = [sys.argv[2]]
    
    # if too many arguments return an error
    elif (len(sys.argv) > 3):
        raise ValueError(f"Too many arguments. Expected 2, received {len(sys.argv)-1}")
        
## FOR MANUAL RUNS USE THESE REPLACEMENTS
# TABLES = ['payments','accounts','stock','clients','users','webusers']
# TABLES = ['applications']

# Update the Yellow DB tables
for provider in PROVIDERS:
    """ Loop through each provider and the appropriate tables and run sync and upload """
    print('------')
    print(provider)

    # if tables in blank, fill it with all
    if not TABLE_ARG:
        TABLES = providers_config[provider].get('tables',[]).keys()

    # Loop through tables, try sync and catch failures to email
    for table in TABLES:
        try:
            tablesync = TableInterface(provider,table)
            tablesync.syncdbtable()
        except Exception as e: 
            # show failure
            print(f"{provider} {table} download and DB sync failed.")
            print(e)
            print('-------------------------------------------------')
            # print traceback
            # traceback.print_exc()
            # send an email notifying failure
            gmail.quick_send(
                to = 'devops@yellow.africa',
                subject = f"DB sync event failed: {provider}.{table}",
                text = f"See AWS log for details <br>{e}",
            )         

### update zoho
# loop through each table configured for zoho release
env = config('env')
if env == 'prod':            
    for zoho_table in ZOHO_TABLES:
        print("--------------------------------------")
        print(f"Zoho Import Sync: {zoho_table}")
        zohoSync(zoho_table, zoho)

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