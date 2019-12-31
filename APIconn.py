""" Create objects to fetch CRM data from all suppliers  

        - connect to api with user and password
        - fetch data and return csv
        - create iterator with mapped headings
"""

# system
import os, json
from io import StringIO
import csv

# third party
import requests
from requests.auth import HTTPBasicAuth

# request = requests.get(url, headers = {'x-api-key':'GKkVWeKIRIhauVbgzsD568ZBAetrJMlv'})

class APIconn:
    """ initalise Angaza API object to make snapshot, api updates etc. """
    def __init__(self, provider):
        # get config for provider
        with open('config.json', 'r') as f:
            cfg = json.load(f)[provider]
        # create api connection
        self.user = cfg.get('username',None)
        self.pswrd = cfg.get('password',None)
        self.headers = cfg.get('headers',None)
        self.snapshoturl = cfg.get('snapshoturl',None)
        self.apiurl = cfg.get('apiurl',None)
        self.sep = cfg.get('sep',",")
        self.tables = cfg['tables']

    def pullSnapshot(self, tablename):
        """ Download table from snapshot URL and correct for bad characters """
        # crete url based on tablename
        url = self.snapshoturl+"/"+self.tables[tablename] 
        if (self.headers is None):
            snapshot = requests.get(
                f"{self.snapshoturl}/{tablename}", 
                auth=HTTPBasicAuth(self.user, self.pswrd))
        else:
            snapshot = requests.get(
                url, 
                headers = self.headers)
        # If successful then return the string
        if snapshot.status_code == 200:
            file_ = snapshot.content.decode('utf-8')
            # lines = file_.splitlines()
            return(file_)
        else:
            raise ValueError(
                "Request to " + tablename 
                + " failed with error code: " + str(snapshot.status_code)
                )