""" Synchonisation objects for API and DB connections """

# System
from io import StringIO
import json

# Third party libraries
import pandas as pd
import numpy as np
from datetime import datetime

# Connection modules
from APIconnections import providerAPI
from yellowpgdb import yellowpgdb
from mapping import headerMap
from config import config

class TableInterface:
    """ Table interface object to perform sync functions
            Provider    : name of provider from which data is fetched
            Table       : table name in the core
            Core        : name of core schema in DB
            Org         : organisation for which the sync is happening
            map_df      : pandas dataframe with mapping for provider table
            map_df_sys  : pandas dataframe with mapping for core table
    """
    # initialise
    def __init__(self, provider, table, core = 'core', org = 'yellow'):
        self.provider = provider
        self.table = table
        self.org = org
        self.core = core
        self.map_df = headerMap(self.table, self.provider, sys=False)
        self.map_df_sys = headerMap(self.table, self.provider, sys=True)
        self.core_map = headerMap(self.table, self.provider, sys=True, core = True)
        self.zoho_map = headerMap(self.table, self.provider, sys=True, zoho = True)
        self.update_on = (self.map_df
                            .loc[self.map_df['update_on']==1,self.org].iloc[0])
        self.pk = (self.core_map
                    .loc[self.core_map['pk']==1,self.org].iloc[0])
        self.db = yellowpgdb()
        self.

    def fetchAndUploadProviderData(self):
        """  Function to sync a specifc table from specific provider 
                table       : name of core table (mapped from config for table name)
                provider    : name of provider
                map_df      : read from sync_mapping csv
                org         : optional, default to yellow (determines org in DB)
        """

        print("-----------------------------------------------------")
        print("Current Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # sync config
        sync_cfg = config(filename='sync.json', section='providers')[self.provider]

        # provider connection and snapshots
        print(f"Fetching {self.provider} {self.table} file...")
        apiconn = providerAPI(self.provider)
        snapshot = apiconn.pullSnapshot(sync_cfg['tables'][self.table])

        # process file
        print("Processing file...")
        instream = StringIO(snapshot)
        df = (pd.read_csv(instream, dtype = str, 
                            na_values=['None','none','NONE',""])
                .replace('[\\t\\r\\n<>&\+]','',regex=True) 
                .replace(np.nan,'\\N')
            )
        df = df[self.map_df[self.provider].values.tolist()]
        df.columns = self.map_df[self.org].values.tolist()
        df = df.dropna(axis = 1, how = 'all')

        # add new fields
        df['external_sys'] = self.provider

        # Process location from lat long to long lat (for PostGIS)
        geo_cols = (self.map_df
                    .loc[self.map_df['type']=='GEOGRAPHY',self.org]
        )
        for col in geo_cols:
            df[col] = df[col].apply(lambda x: " ".join(x.split(',')[::-1]))

        # Re-create table header in case different
        print("Re-creating table header...")
        db_engine = self.db.get_engine()
        df.head(0).to_sql(
            self.table, 
            db_engine,
            schema = self.provider, 
            if_exists='replace',
            index=False
            )

        # Create stringIO stream, and set cursore to start
        outstream = StringIO()
        df.to_csv(outstream, sep='\t', header=False, index=False)
        outstream.seek(0)

        # Upload contents
        print("Uploading contents...")
        db = yellowpgdb()
        db_conn = db.connect()
        cur = db_conn.cursor()
        cur.copy_from(outstream, f"{self.provider}.{self.table}") 
        db_conn.commit()

        print("Current Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("-----------------------------------------------------")

    def _createSelect(self, row):
        dtype = row['type']
        col = row[self.org]
        select = f"a.{col}"
        if (dtype not in ('TEXT','GEOGRAPHY')):  
            select = f"cast(a.{col} as {dtype}) as {col}"
        elif (dtype == 'GEOGRAPHY'):
            select = f"ST_GeographyFromText('POINT('||{col}||')') as {col}"
        return(select)

    def _createUpdate(self, row):
        dtype = row['type']
        col = row[self.org]
        select = f"{col} = b.{col}"
        if (dtype not in ('TEXT','GEOGRAPHY')):  
            select = f"{col} = cast(b.{col} as {dtype})"
        elif (dtype == 'GEOGRAPHY'):
            select = f"{col} = ST_GeographyFromText('POINT('||b.{col}||')')"
        return(select)

    
    def _insertTableStatement(self):
        # create the cast statements
        self.map_df_sys['select'] = (
            self.map_df_sys.apply(
                self._createSelect, axis = 1)
        )
        casts = ', '.join(self.map_df_sys['select'].values.tolist())
        # generate a select statement
        select = f"SELECT {casts} FROM {self.provider}.{self.table} a"
        # general comma seperated column list
        cols_csv = ", ".join(self.map_df_sys[self.org].values.tolist())
        # generate the insert and where clause
        insert = f"INSERT INTO {self.core}.{self.table} ({cols_csv})"
        where = (f"""WHERE a.{self.update_on} 
                    NOT IN (select c.{self.update_on} 
                    FROM {self.core}.{self.table} c)"""
        )
        # join parts with new line chars
        insert_sql= "\n".join([insert, select, where])
        return(insert_sql)

    # 2. update 
    def _updateTableStatement(self):
        """ create an update statement to sync tables """
        # update header
        update = f"UPDATE {self.core}.{self.table} a"
        # create casts
        self.map_df_sys['update'] = (
            self.map_df_sys.apply(self._createUpdate, axis = 1)
        )
        casts = ', '.join(
            self.map_df_sys['update'].values.tolist()+
            ["change_timestamp = CURRENT_TIMESTAMP"])  # need to set change timestamp
        # set statements for each col
        sets = f"SET {casts} FROM {self.provider}.{self.table} b"
        where = f"WHERE a.{self.update_on} = b.{self.update_on}"
        update_sql = "\n".join([update, sets, where])
        return(update_sql)

    def internalSync(self):
        """ insert new and update existing records to core tables """
        # 1. insert all new
        print("Creating insert statement...")
        insert_sql = self._insertTableStatement()
        print("Executing insert statement...")
        self.execute(insert_sql)
        # 2. update all the rest
        print("Creating update statement...")
        update_sql = self._updateTableStatement()
        print("Executing update statement...")
        self.execute(update_sql)
        print("Internal Sync Complete.")
        print("-----------------------------------------------------")

    def _castTZasTS(self, mapping):
        def applyCast(row):
            s = (row[self.org] if (row['type'] != 'TIMESTAMPTZ') 
                else f"cast ({row[self.org]} as TIMESTAMP) {row[self.org]}")
            return(s)    
        return(mapping.apply(applyCast, axis = 1))

    def fetchCoreTableSQL(self, update=False):
        """ get a core table with our without zohoid for insert or update"""

        # if update or insert, need to set the where clause
        is_or_not = "is not" if update else "is"
        is_or_not = "is not" if update else "is"

        #list of columns from mapping
        cols = ", ".join(self._castTZasTS(self.zoho_map))
        # sql command
        sql = f"""
                set timezone TO 'Africa/Johannesburg';
                select {cols}
                from {self.core}.{self.table} a
                where a.zoho_id {is_or_not} null
                order by {self.pk}
            """
        # fetch df
        # df = pd.read_sql(sql, self.db_conn)
        return(sql)

    def connect(self):
        self.db_conn = self.db.connect()

    def execute(self, sql):
        self.connect()
        cursor = self.db_conn.cursor()
        cursor.execute(sql)
        self.db_conn.commit()
        return(cursor)

    def close(self):
        self.db_conn.close()


if __name__ == "__main__":
    sync = TableInterface('angaza','clients')
    # sync.fetchAndUploadProviderData()
    # sync.internalSync()
    # x = pd.DataFrame(sync.selectData())
    # print(x.head())

# // "receipts":"receipts"
# // "applications":"prospects"
# // "clients":"clients"