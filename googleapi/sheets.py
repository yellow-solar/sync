""" Set of functions to interact with Google Sheets API
        - read 

 """

#  standard
import os, json, base64, re

# third party
import pandas as pd
from pandas.io.json import json_normalize
from googleapiclient import discovery, errors
from google.oauth2 import service_account
from google.auth.transport.requests import Request

SAMPLE_SPREADSHEET_ID = '1l0rxY-SR0e9vW8n4Ox4-EwRC-H3JvdoAL6fDb3Z86FY'
SAMPLE_RANGE = "Agent_Recruitment_Test1"

FILENAME = 'agent-recruitment-sync-c26ca6015141.json'
CONFIG_FOLDER = 'config'

# Google drive API config
class GSheet():
    """gmail service creator with input account file
            INPUT: SHEETS API private key file path and name
    """
    def __init__(self, filename):
        self.filename = filename
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly',
                        ]
        self.credentials = (service_account
            .Credentials.from_service_account_file(
                self.filename, 
                scopes=self.scopes
            )
        )

        # Initiate google service 
        self.service = discovery.build('sheets', 'v4',
            credentials=self.credentials)
        self.sheet_values = self.service.spreadsheets().values()
        
    def read(self, sheetID, cellrange):
        """ Read specified range from the Sheets API, return list of values """
        result = self.sheet_values.get(spreadsheetId=sheetID,
                                    range=cellrange).execute()
        values = result.get('values', [])
        return (values)

    def readBatchDFs(self, sheetID, sheet_names):
        """ Return dictionary of pandas DFs from a dict of ranges 
                ranges can just be sheet names, return 'UsedRange'
            """
        # call API to get batch result in JSON format
        batch_result = self.sheet_values.batchGet(
            spreadsheetId = sheetID,
            ranges = sheet_names,
            # majorDimension = "COLUMNS",
            ).execute()
        return(batch_result) 

def processBatchDF(batch_result, sheets_config):
    # create dictionary of dateframes to send back, 
    # keys are same as rangedict
    df_dict = {}
    # loop through batch results
    for result in batch_result.get('valueRanges',[]):
        form = re.sub(r'!.*', '', result['range'])
        config = sheets_config.get(form,{})
        # create df from values returned by google for the specific results
        result_values = result['values']
        df = pd.DataFrame(result_values[1:]) 
        df.columns = result_values[0][0:len(df.columns)]
        # set columns to an first row, replacing spaces with underscores
        # and only alphanumeric characters
        # check if columns requested are in google sheet
        if config.get('columns'):
            sheet_cols = config.get('columns',[])
            for col in sheet_cols:
                if col not in df.columns.values:
                    raise Exception(f"{col} not in sheet columns")
            # if no error, filter the columns to sheet_cols
            df = df[sheet_cols]
        else:
            sheet_cols = df.columns.values
        # Replace whitespace and remove non-alphanumerics
        sheet_cols_alphanum = [re.sub(r'\W',' ',col).rstrip() for col in sheet_cols] 
        df.columns = [re.sub(r' +',' ',col).replace(' ','_') for col in sheet_cols_alphanum] 
        # drop empty column names, if any
        if '' in df.columns:
            df = df.drop('',axis=1)
        # convert columns to numeric where possible,
        # ignores columns that have some strings, leaving as object
        # for col in df.columns:
        #     df[col]= pd.to_numeric(df[col], errors='ignore')
        # convert input timestamp columns
        for col in config.get('timestamps',[]):
            if col in df.columns:
                df[col]= pd.to_datetime(df[col])
            else:
                raise KeyError(f"Timestamp columns {col} not in index")
                    # .strftime("%Y-%m-%d %H:%M:%S")
        # add to df dictionary
        df_dict[form] = df 
    
    #return the dictionary of dataframes
    return (df_dict) 
    
if __name__ == '__main__':
    # Initialise google sheets API
    gsheet = GSheet(CONFIG_FOLDER + '/' + FILENAME)

    # Test single range
    values = gsheet.read(SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE)
    if not values:
            print('No data found.')
    else:
        for row in values[100]:
            # Print columns A and E, which correspond to indices 0 and 4.
            print(row)  

    # Test multiple ranges
    gsheet.readBatchDFs(
        sheetID = '1l0rxY-SR0e9vW8n4Ox4-EwRC-H3JvdoAL6fDb3Z86FY', 
        sheet_names = ['Agent_Recruitment_Test1',
                        ]
    )
    # batch = gsheet.read(SAMPLE_SPREADSHEET_ID, 
    #     , {}
    #     )  