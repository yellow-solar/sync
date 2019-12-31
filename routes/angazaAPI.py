""" Objects to interact with the Angaza API 

        - grab snapshot and upload to our DB
        - grab records with conditions/recent and update our DB
            - maybe page through all of them
        - payments API interaction

"""

# system
import os, json
from io import StringIO
import csv

# third party
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import numpy as np

# local?

class AngazaAPI:
    """ initalise Angaza API object to make snapshot, api updates etc. """
    def __init__(self):

        os.environ['env'] = 'd'
        with open('config/config.json', 'r') as f:
            cfg = json.load(f)['angaza']

        self.user = cfg['username']
        self.pswrd = cfg['password']
        self.snapshot = False
        self.snapshoturl = 'https://payg.angazadesign.com/api/snapshots'
        self.apiurl = 'https://payg.angazadesign.com/data'

    def pullSnapshot(self, tablename):
        """ Download table from snapshot URL and correct for bad characters """
        snapshot = requests.get(
            f"{self.snapshoturl}/{tablename}", 
            auth=HTTPBasicAuth(self.user, self.pswrd))
        if snapshot.status_code == 200:
            snapshot_df = pd.read_csv(StringIO(snapshot.content.decode('utf-8')))
            # Replace bad Nones and characters before saving
            snapshot_df = (snapshot_df
                .replace('None',np.NaN)
                .replace('none',np.NaN)
                .replace('NONE',np.NaN)
                .replace("&","and",regex=True)
                .replace("<","",regex=True)
                .replace(">","",regex=True)
            )
            return(snapshot_df)
        else:
            raise ValueError(
                "Request to " + tablename 
                + " failed with error code: " + str(snapshot.status_code)
                )

    def pullSnapshotCSVonly(self, tablename):
        """ Download table from snapshot URL and correct for bad characters """
        snapshot = requests.get(
            f"{self.snapshoturl}/{tablename}", 
            auth=HTTPBasicAuth(self.user, self.pswrd))
        if snapshot.status_code == 200:
            return(StringIO(snapshot.content.decode('utf-8')))
        else:
            raise ValueError(
                "Request to " + tablename 
                + " failed with error code: " + str(snapshot.status_code)
                )


# Test
# angaza = AngazaAPI()
# df = angaza.pullSnapshot(tablename = 'payments')
# print(df.tail())