""" Program to get snapshot, upload to PostgresDB  """

# System
from io import StringIO
import json

# Third party libraries
import pandas as pd
import numpy as np
from datetime import datetime

# Connection modules
from APIconn import APIconn
from yellowpgdb import yellowpgdb
from mapping import headerMap
from config import config

def fetchAndUploadProviderData(table, provider, colmapping, org='yellow'):
    """  Function to sync a specifc table from specific provider 
            table       : name of core table (mapped from config for table name)
            provider    : name of provider
            colmapping  : read from sync_mapping csv
            org         : optional, default to yellow (determines org in DB)
    """

    print("-----------------------------------------------------")
    print("Current Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # sync config
    sync_cfg = config(filename='sync.json', section='providers')[provider]

    # provider connection and snapshots
    print(f"Fetching {provider} {table} file...")
    apiconn = APIconn(provider)
    snapshot = apiconn.pullSnapshot(sync_cfg['tables'][table])

    # process file
    print("Processing file...")
    lines = StringIO(snapshot)
    df = (pd.read_csv(lines, dtype = str, 
                        na_values=['None','none','NONE',""])
            .replace('[\\r\\n<>&\+]','',regex=True) 
            .replace(np.nan,'\\N')
        )
    df = df[colmapping[provider].values.tolist()]
    df.columns = colmapping[org].values.tolist()
    df = df.dropna(axis = 1, how = 'all')

    # add new fields
    df['external_sys'] = provider

    # Process location from lat long to long lat (for PostGIS)
    geo_cols = colmapping.loc[colmapping['type']=='GEOGRAPHY',org]
    for col in geo_cols:
        df[col] = df[col].apply(lambda x: " ".join(x.split(',')[::-1]))

    # Re-create table header in case different
    print("Re-creating table header...")
    db = yellowpgdb()
    db_engine = db.get_engine()
    df.head(0).to_sql(
        table, db_engine,
        schema = provider, 
        if_exists='replace',
        index=False
        )

    # Create stringIO
    lines = StringIO()
    df.to_csv(lines, sep='\t', header=False, index=False)
    lines.seek(0)

    # Upload contents
    print("Uploading contents...")
    db = yellowpgdb()
    db_conn = db.connect()
    cur = db_conn.cursor()
    cur.copy_from(lines, f"{provider}.{table}") 
    db_conn.commit()

    print("Current Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("-----------------------------------------------------")

# conn = APIconn('upya')
# snapshot = conn.pullSnapshot('users')
# print(len(snapshot))
# print(snapshot)

if __name__ == "__main__":
    TABLE = 'users'
    PROVIDER = 'angaza'
    ORG = 'yellow'
    colmapping = headerMap(TABLE, PROVIDER, sys=False)
    fetchAndUploadProviderData(TABLE, PROVIDER, colmapping)

    # sync_cfg = config(filename='sync.json', section='providers')[PROVIDER]
    # print(sync_cfg)