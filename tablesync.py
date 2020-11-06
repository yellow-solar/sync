""" Synchonisation objects for API and DB connections """

# System
from io import StringIO
import json

# Third party libraries
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extensions import AsIs
import csv

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
        self.core_map = headerMap(self.table, self.provider, sys=True, core = True)
        self.zoho_map = headerMap(self.table, self.provider, sys=True, zoho = True)
        self.pk = (self.core_map
                    .loc[self.core_map['pk']==1,self.org].iloc[0])
        self.update_on = (self.core_map
                            .loc[self.core_map['update_on']==1,self.org].iloc[0])
        # Database stuff
        self.db = yellowpgdb()
        self.core_cfg = config('solarcore').get(self.table, None)
        self.update_sql = self.core_cfg.get('update_query',None)
        self.connect()
        # Dataframe
        self.df = pd.DataFrame()
        self.apifile = None
        # For provider data
        if self.provider is not None:
            self.provider_cfg = config(section = "providers")[provider]
            self.table_cfg = self.provider_cfg['tables'].get(table,"")
            self.map_df = headerMap(self.table, self.provider, sys=False)
            self.map_df_sys = headerMap(self.table, self.provider, sys=True)

    def fetchAndUploadProviderData(self):
        """  Function to sync a specifc table from specific provider 
                table       : name of core table (mapped from config for table name)
                provider    : name of provider
                map_df      : read from sync_mapping csv
                org         : optional, default to yellow (determines org in DB)
        """

        print("-----------------------------------------------------")
        print("Current Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
           
        # Fetch Data
        self._fetchAndConvertProviderData()
        # Continue if data
        if (len(self.apifile)>0): 
            self._processProviderData()

            # Re-create table header in case different
            print("Re-creating table header...")
            db_engine = self.db.get_engine()
            self.df.head(0).to_sql(
                self.table, 
                db_engine,
                schema = self.provider, 
                if_exists='replace',
                index=False
                )

            # Create stringIO stream, and set cursore to start
            outstream = StringIO()
            # replace null and stream out
            self.df.replace(np.nan,'\\N').to_csv(outstream, sep='\t', header=False, index=False, quoting = csv.QUOTE_NONE)
            outstream.seek(0)

            # Upload contents
            print("Uploading contents...")
            db = yellowpgdb()
            db_conn = db.connect()
            cur = db_conn.cursor()
            cur.copy_from(outstream, f"{self.provider}.{self.table}") 
            db_conn.commit()
            db_conn.close()

            print("Current Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            print("-----------------------------------------------------")
        else:
            print("No values in file")

    def _fetchAndConvertProviderData(self):
        # provider connection and snapshots
        print(f"Fetching {self.provider} {self.table} files...")
        apiconn = providerAPI(self.provider)

        # Loop through if multiple call
        if self.table_cfg.get("iter",False):
            count = 0
            for iter_value in self.table_cfg['iterValues']:
                # Track file number
                count+=1
                print(f"{count} of {len(self.table_cfg['iterValues'])}: {iter_value}")
                
                # Data/body of request
                data = self.table_cfg.get("restOfBody",{})
                data[self.table_cfg['iterVarName']] = iter_value
                
                # Range inputs 
                if self.table_cfg.get("range",None) is not None:
                    if self.table_cfg.get("rangeType") == "subdays" and '%fromdate' in data.values():
                        from_date = datetime.now() - timedelta(days=self.table_cfg.get("range"))
                        from_date_str = from_date.strftime("%Y-%m-%d")
                        from_date_key = {y:x for (x,y) in data.items()}['%fromdate']
                        data[from_date_key] = from_date_str
                
                # Send post request, return file if successful
                self.apifile = apiconn.pullSnapshot(self.table_cfg['url'], get=False, post=True,data=data)
                if (len(self.apifile) >0):
                    self._convertFileStringAppendToDF()
        else:
            if self.table_cfg.get("requiresBody",False):
                data = self.table_cfg['body']
            else:
                data=None
            self.apifile = apiconn.pullSnapshot(self.table_cfg['url'], data=data)
            if (len(self.apifile) >0):
                    self._convertFileStringAppendToDF()
            
    def _convertFileStringAppendToDF(self):
        # process file if values
        instream = StringIO(self.apifile)
        df = (pd.read_csv(instream,
                            sep = self.provider_cfg.get("sep",','), 
                            dtype = str,
                            na_values=['n/a','None','none','NONE',"",'n/a;n/a'])
                .replace('[\\t\\r\\n<>&]','',regex=True) 
            )

        # Append to existing df
        self.df = self.df.append(df, sort = False)

    def _processProviderData(self): 
        """ Function to process certain columns in download
        """

        # concatenate columns with + in them
        to_concat = self.map_df[self.provider].str.contains("+",regex=False)
        for index in self.map_df[to_concat].index:
            concat_col_name = self.map_df.loc[index, self.provider]
            self.df[concat_col_name] = ""
            concat_cols = self.map_df.loc[index, self.provider].split("+")
            for col in concat_cols:
                self.df[concat_col_name] = (
                    self.df[concat_col_name]
                        .str.cat(self.df[col], sep = " ")
                        .str.lstrip()
                ) 

        # concatenate columns with | in them
        to_concat = self.map_df[self.provider].str.contains("|",regex=False)
        for index in self.map_df[to_concat].index:
            concat_col_name = self.map_df.loc[index, self.provider]
            self.df[concat_col_name] = ""
            concat_cols = self.map_df.loc[index, self.provider].split("|")
            for col in concat_cols:
                self.df[concat_col_name] = (
                    self.df[concat_col_name]
                        .str.cat(self.df[col], sep = "")
                        .str.lstrip()
                ) 

        # get the mapped columns that are actually in the df
        mapped_cols_in_df = [x for x in self.map_df[self.provider].values.tolist() if x in self.df.columns]
        # filter table for cols that are mapped
        self.df = self.df[mapped_cols_in_df]
        # get the target colnames that exist
        renamed_cols_in_df = self.map_df.loc[self.map_df[self.provider].isin(mapped_cols_in_df), self.org].values.tolist()
        # rename for re entry to DB 
        self.df.columns = renamed_cols_in_df
        # drop any blank rows
        self.df = self.df.dropna(axis = 1, how = 'all')

        # Process location from lat long to long lat (for PostGIS)
        geo_cols = (self.map_df
                    .loc[self.map_df['type']=='GEOGRAPHY',self.org]
        )
        for col in geo_cols:
            self.df[col] = self.df[col].replace("- -",np.NaN)
            self.df[col] = self.df[col].apply(
                lambda x: " ".join(x.split(',')[::-1]) if x is not np.NaN else x
            )

        # add new fields
        self.df['external_sys'] = self.provider
        self.df['organization'] = self.provider_cfg['organization']
        self.df['country'] = self.provider_cfg['country']

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
        select = f"{col} = a.{col}"
        if (dtype not in ('TEXT','GEOGRAPHY')):  
            select = f"{col} = cast(a.{col} as {dtype})"
        elif (dtype == 'GEOGRAPHY'):
            select = f"{col} = ST_GeographyFromText('POINT('||a.{col}||')')"
        return(select)

    def _insertTableStatement(self, filter_insert = None):
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
                    FROM {self.core}.{self.table} c)
                """
        )
        if filter_insert is not None:
            where = where + " and " + filter_insert

        # join parts with new line chars
        insert_sql= "\n".join([insert, select, where])

        return(insert_sql)

    # 2. update 
    def _updateTableStatement(self, filter_update):
        """ create an update statement to sync tables """
        # update header
        update = f"UPDATE {self.core}.{self.table} c"
        # create casts
        self.map_df_sys['update'] = (
            self.map_df_sys.apply(self._createUpdate, axis = 1)
        )
        casts = ', '.join(
            self.map_df_sys['update'].values.tolist()+
            ["change_timestamp = CURRENT_TIMESTAMP"])  # need to set change timestamp
        # set statements for each col
        sets = f"SET {casts} FROM {self.provider}.{self.table} a"
        where = f"WHERE c.{self.update_on} = a.{self.update_on}"
        
        if filter_update is not None:
            where = where + " and " + filter_update
        
        update_sql = "\n".join([update, sets, where])
        return(update_sql)

    def internalSync(self):
        """ insert new and update existing records to core tables 
                - if not users then extend table
                - if users then update on user_email
        """
        # 0. update self.map with actual columns that are in the staging table
        cols = self._getStagingColNames()
        self.map_df_sys = self.map_df_sys.loc[self.map_df_sys[self.org].isin(cols)]
        # 1. insert all new
        print("Creating insert statement...")
        insert_sql = self._insertTableStatement(self.table_cfg.get('filter',None))
        print("Executing insert statement...")
        self.execute(insert_sql)
        # 2. update all the rest
        print("Creating update statement...")
        update_sql = self._updateTableStatement(self.table_cfg.get('filter',None))
        print("Executing update statement...")
        self.execute(update_sql)
        print("Internal Sync Complete.")
        print("-----------------------------------------------------")

    def _castTZasTS(self, mapping):
        def applyCast(row):
            if (row['type'] != 'TIMESTAMPTZ'):
                s = f"a.{row[self.org]}"
            else: 
                s = f"cast (a.{row[self.org]} as TIMESTAMP) {row[self.org]}"
            return(s)    
        return(mapping.apply(applyCast, axis = 1))

    def fetchCoreTableSQL(self, update=False):
        """ get a core table with our without zohoid for insert or update"""
        # if update or insert, need to set the where clause
        is_or_not = "is not" if update else "is"
        #list of columns from mapping
        cols = ", ".join(self._castTZasTS(self.zoho_map))
        # sql command
        sql = f"""
                select {cols}
                from {self.core}.{self.table} a
                where a.zoho_id {is_or_not} null
                    and a.archived = false
                order by {self.pk}
            """
        if update and self.update_sql is not None:  
            sql = self.update_sql['sql'].format(cols)
        return(sql)

    def corePrepSQL(self):
        if self.table_cfg.get('core_prep',False):
            print("Running core updates before internal sync")
            self.execute(open("sql/coreprep.sql", "r").read())
        elif self.table_cfg.get('users_prep',False):
            print("Running users updates before internal sync")
            self.execute(open("sql/usersprep.sql", "r").read())

    def coreUpdateSQL(self):
        if self.table_cfg.get('coreUpdateBool',False):
            print("Running core updates after internal sync")
            file_ = "sql/" + self.table_cfg['coreUpdateSQL']
            print(f"{file_}...")
            self.execute(open(file_, "r").read())

    def _getStagingColNames(self):
        curs = self.db_conn.cursor()
        tablepath = self.provider + "." + self.table
        curs.execute('select * FROM %s.%s LIMIT 0', (AsIs(self.provider), AsIs(self.table)))
        colnames = [desc[0] for desc in curs.description] 
        return(colnames)

    def connect(self):
        self.db_conn = self.db.connect()

    def execute(self, sql):
        self.connect()
        with self.db_conn as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
        return(cursor)

    def close(self):
        self.db_conn.close()

    def syncdbtable(self):
        self.fetchAndUploadProviderData()
        self.corePrepSQL()
        self.internalSync()
        self.coreUpdateSQL()

if __name__ == "__main__":
    sync = TableInterface('upya','payments')
    print(sync.fetchCoreTableSQL(update=True))
    # sync.fetchAndUploadProviderData()
    # sync.internalSync()
    # x = pd.DataFrame(sync.selectData())
    # print(x.head())

# // "receipts":"receipts"
# // "applications":"prospects"
# // "clients":"clients"