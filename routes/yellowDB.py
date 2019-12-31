"""Get the standard bank cashflows from Zoho

Download via ZohoAPI and then add new records to relevant finance Database

Note: 
    # Config for zoho sync calls - the report view name is what is shown on the dashboar, 
    # while the link name is the API name, and seen on the Zoho API reference page

"""
# Standard and 3rd party libary imports
import os
import json
from datetime import datetime
from pandas.io.json import json_normalize
import sqlalchemy as db
import pandas as pd
import csv

# Local application imports
from yellowsync.API.zohoAPI import ZohoAPI
from yellowsync.API.angazaAPI import AngazaAPI


class yellowDB1:
    """ Class that creates the objects for the Yellow DB engine
    """
    def __init__(self, schema):
        # Database config
        # Access the config for DB
        with open('config/config.json', 'r') as f:
            db_cfg = json.load(f)[f"yellowdb{os.environ['env']}"]

        # Create engine for connections pool
        self.engine = db.create_engine(
            f"{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/{schema}?charset=utf8mb4", 
            # , echo = True
            )

    def CSVtoDB(self, csv):
        print(csv)
        header = csv[0].split(',')
        print(header)
        for line in csv[1:]:
            print(line)

def yellowDBSync(table, schema, insert_cols=None, 
                insert_cols_rename=None, index_label = None, 
                form_link=None, if_exists='replace', df = None, dl=True):
    """ Download or import and sync table in Yellow DB """
    # if there is a csv in the filename, import rather than fetch:
    if schema == 'Angaza':
        angaza = AngazaAPI()
        if dl:
            print(f'Downloading {table}')
            insert_df = angaza.pullSnapshot(tablename = table)
        else:
            print('Using provided data for sync to YellowDB')
            insert_df = df

    elif schema == 'Mobile':
        insert_df = df

    # else need to fetch from Zoho
    elif schema == "Zoho":
        # If form_link is not given, set to form name
        if form_link is None:
            form_link = table
        
        # Fetch zoho cfg and setup API connection
        with open('config/config.json', 'r') as f:
            zoho_cfg = json.load(f)['zoho']
        zoho = ZohoAPI(zoho_cfg['zc_ownername'], zoho_cfg['authtoken'], zoho_cfg['app'])

        # Get the records from the Zoho form
        print(f"Downloading zoho table {table}...")
        form_view = zoho.get(table, payload={'raw':'true',}) # via the trigger table
        if form_view.status_code == 200:
            if "errorlist" in form_view.text.lower():
                raise Exception (f"Errorlist return in Zoho request for {table}")
            elif "no such view" in form_view.text.lower():
                raise Exception("No such table or form link")
            else:
                try:
                    form_json = json.loads(form_view.text) 
                except:
                    raise
        else:
            raise Exception (f"Request returned error code {form_view.status_code} in Zoho request for {table}")

        # Convert JSON to pandas dataframe
        insert_df = json_normalize(form_json[form_link])

        # Save current in csv
        insert_df.to_csv(f'../data/{table}.csv')

        # Assert len > 1
        assert len(insert_df)>0

    else:
        raise ValueError(f"{schema} is not a valid Yellow DB schema")

    ### Reformat header if requested:
    if insert_cols is not None:
        insert_df = insert_df[insert_cols]
    if insert_cols_rename is not None:
        insert_df = insert_df.rename(columns=insert_cols_rename)

    ## Shorten column length to 64 characters for MySQL
    insert_df.columns = [x[0:64] if (len(x) > 64) else x for x in insert_df.columns ]

    ### Upload to database
    # Database config
    # Access the config for DB
    with open('config/config.json', 'r') as f:
        db_cfg = json.load(f)[f"yellowdb{os.environ['env']}"]

    # Create engine for connections pool
    engine = db.create_engine(
        f"{db_cfg['driver']}{db_cfg['user']}:{db_cfg['passwd']}@{db_cfg['host']}/{schema}?charset=utf8mb4", 
        # , echo = True
        )
    
    # If appending rows to DB table then need an added time column
    if if_exists != 'replace':
        insert_df['added_datetime'] = datetime.now()
        table = table+'_Backup'
        
    # Insert full table
    with engine.connect() as connection:
        insert_df.to_sql(con=connection, name=table, 
            index_label = index_label,
            if_exists=if_exists, index=False)

    print(table + " inserted")
    status = 200
    return(status)



    