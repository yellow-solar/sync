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

def insertOrUpdateZoho(tablesync, zoho, form, update, slice_length):
    """ YDB <> Zoho insert or update using Pandas DF"""
    
    # set update criteria if required
    update_on = tablesync.update_on if update else None

    # fetch DF
    sql = tablesync.fetchCoreTableSQL(update=update)
    cur = tablesync.db_conn.cursor()
    cur.execute(sql)
    # process results
    records = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]

    # list of dicts which are zipped with field name
    record_dicts = [dict(zip(colnames, record)) for record in records]
    
    # upload/insert df
    dicts_slices = ([record_dicts[i:i + slice_length] 
                    for i in range(0, len(record_dicts), slice_length)])
    for dicts_slice in dicts_slices:
        # call zoho with XML
        response = zoho.rpcUploadChunk(dicts_slice, form, update_on = update_on)
        insert_or_update = "Updated" if update else "Inserted"
        print(f"{insert_or_update} {len(dicts_slice)} records to Zoho")

        if not update:
            try:
                # process response to get ids
                print("Creating ID maps..")
                ids = zoho.xmlFindIDs(response, tablesync.update_on)
                # update DB with each ZOho ID 
                print("Inserting IDs...")
                
                #TODO: build a progress tracker
                for row in ids[['ID',tablesync.update_on]].itertuples(index=False, name=None):
                    cur = tablesync.db_conn.cursor()
                    update_sql = f""" update {tablesync.core}.{tablesync.table}
                        set zoho_id = %s
                        where {tablesync.update_on} = %s
                    """
                    cur.execute(update_sql, row)
                tablesync.db_conn.commit()
                print(f"Inserted {len(ids)} IDs to YDB")
            except:
                ### if it excepts then we need to delete the new inserts and IDs?
                raise Exception("Failed to process response")


if __name__ == "__main__":
    """
    when Cron calls the zoho script, it must call with the form 
    name input 
    """

    PROVIDER = 'angaza'
    
    # assign form from sys.args (1st is the )
    if len(sys.argv) > 1:
        table = sys.argv[1]
    else:
        # raise Exception("Expecting form as argument to call upload")
        table = 'accounts'

    # zoho syc config
    zohosync_cfg = config(filename='sync.json', section='zoho')[table]
    form = zohosync_cfg['form']
    slice_length = zohosync_cfg['slice_length']

    # YDB table config
    tablesync = TableInterface(PROVIDER,table)
    
    # 1. Update Providers > DB
    # tablesync.fetchAndUploadProviderData()
    # tablesync.internalSync()

    # 2. Update Zoho <> YDB
    tablesync.connect()

    # Fetch zoho cfg and setup API connection
    zoho_cfg = config(filename='config.json', section='zoho')
    zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])

    # update the old records to Zoho - update first because inserts don't need to be
    insertOrUpdateZoho(tablesync, zoho, form=zohosync_cfg['form'], update=True, slice_length = slice_length)

    # insert the new records to Zoho
    insertOrUpdateZoho(tablesync, zoho, form=zohosync_cfg['form'], update=False, slice_length = slice_length)

    
