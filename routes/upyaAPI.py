""" Objects to interact with the Upya API 

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


url = 'https://api.upya.io/download/contracts'

key = 'GKkVWeKIRIhauVbgzsD568ZBAetrJMlv'

request = requests.get(url, headers = {'x-api-key':'GKkVWeKIRIhauVbgzsD568ZBAetrJMlv'})
snapshot_df = pd.read_csv(StringIO(request.content.decode('utf-8')),sep = ';')
print(snapshot_df.head())

