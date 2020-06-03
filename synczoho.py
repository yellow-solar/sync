""" update DB and then update zoho """

# syslibrary
import os,sys
from datetime import datetime 

# third party
import pandas as pd
import numpy as np
import decimal

# custom imports
import tablesync
from tablesync import TableInterface
from yellowpgdb import yellowpgdb
from config import config
from APIconnections import ZohoAPI
from googleapi.gmail import Gmail

gmail = Gmail('googleservice/mail-93851bb46b8d.json', 'system@yellow.africa')

def insertOrUpdateZoho(tablesync, zoho, form, update, slice_length):
    """ YDB <> Zoho insert or update using Pandas DF"""
    
    # set update criteria if required
    # update_on = tablesync.update_on if update else None
    update_on = 'ID' if update else None
    insert_or_update = "Updated" if update else "Inserted"

    # Buil sql and cursor
    sql = tablesync.fetchCoreTableSQL(update=update)
    cur = tablesync.db_conn.cursor()
    cur.execute(sql)
    # Get results
    records = cur.fetchall()
    colnames = [desc[0] if desc[0]!="zoho_id" else "ID" for desc in cur.description]
    
    # List of dicts which are zipped with field name
    record_dicts = [dict(zip(colnames, record)) for record in records]

    # Convert decimal records to string representation
    for record_dict in record_dicts:
        for key in record_dict.keys():
            if isinstance(record_dict[key], decimal.Decimal):
                record_dict[key] = "{0:0.{prec}f}".format(
                    record_dict[key], 
                    prec = decimal.Decimal(record_dict[key]).as_tuple().exponent*-1
                )

    # Split data into slices for XML length restriction 
    dicts_slices = ([record_dicts[i:i + slice_length] 
                    for i in range(0, len(record_dicts), slice_length)])
                    
    processed = 0
    # Upload/insert data slice
    for j in range (0,len(dicts_slices)):
        dicts_slice = dicts_slices[j]
        # Call zoho with XML
        response = zoho.rpcUploadChunk(dicts_slice, form, update_on = update_on)
        processed += len(dicts_slice)
        print(f"Slice {j+1}: {insert_or_update} {processed} of {len(record_dicts)} records to Zoho")

        # Insert IDs back to DB if not updating
        if not update:
            try:
                # process response to get ids
                print("Creating ID maps..")
                ids = zoho.xmlFindIDs(response, tablesync.update_on)
                # update DB with each ZOho ID 
                print("Inserting IDs...")
                
                # loop through ids and update YDB one at a time
                counter = 0
                for row in ids[['ID',tablesync.update_on]].itertuples(index=False, name=None):
                    cur = tablesync.db_conn.cursor()
                    update_sql = f""" update {tablesync.core}.{tablesync.table}
                        set zoho_id = %s
                        where {tablesync.update_on} = %s
                    """
                    cur.execute(update_sql, row)
                    counter+=1
                    # print(f"Inserted {counter} of {len(ids)}")
                tablesync.db_conn.commit()
                print(f"Inserted {len(ids)} IDs to YDB")
            except Exception as e:
                ### if it excepts then we need to delete the new inserts and IDs?
                # raise Exception("Failed to process response")
                # send an email notifying failure
                gmail.quick_send(
                    to = 'devops@yellow.africa',
                    subject = f"Zoho sync error",
                    text = f"See AWS log for details <br><br>{e}",
                )  

def zohoSync(zoho_table, zoho):
    # 0. Prep
    print("-----------------------------------------------------")
    print("Current Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # zoho syc config
    zohosync_cfg = config(section='zoho')['sync_tables'][zoho_table]
    table = zohosync_cfg['table']
    form = zohosync_cfg['form']
    slice_length = zohosync_cfg['slice_length']
    # update_ydb = zohosync_cfg.get('update_ydb',dbupdate)

    # YDB table config
    tablesync = TableInterface(None,table)
    tablesync.connect()

    # 2. Update Zoho <> YDB
    # Update the old records to Zoho - update first because inserts don't need to be
    insertOrUpdateZoho(tablesync, zoho, form=zohosync_cfg['form'], update=True, slice_length = slice_length)

    # Insert the new records to Zoho
    insertOrUpdateZoho(tablesync, zoho, form=zohosync_cfg['form'], update=False, slice_length = slice_length)

if __name__ == "__main__":
    """
    when cron calls the zoho script, it must call with the form 
    name input
    """
    providers = config(section='providers')
    zoho_tables = config(section='zoho')['sync_tables']  

    # Fetch zoho cfg and setup API connection object
    zoho_cfg = config(section='zoho')
    zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])

    # loop through each table in zoho
    env = config('env')
    if env == 'prod':            
        # for zoho_table in zoho_tables:
        for zoho_table in ['applications']:
            print(f"Zoho Import Sync: {zoho_table}")
            zohoSync(zoho_table, zoho)
    else:
        print("Can only update Zoho in prod")

    # Run the upload sync checker to look for new values
    check = zoho.add("API_Triggers", payload = {"trigger_command":"execute","form":"NA","command_string":"Upload_Sync_Checks"}) 
    if check.status_code==200:
        print("Upload sync checked")
    else: 
        print(check.text)
