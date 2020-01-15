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

# YDB table config
providers = config(filename='sync.json', section='providers')
core_tables = config(filename='sync.json', section='core')

# for provider in providers:  
#     print('------')
#     print(provider)
#     for table in providers[provider].get('tables',[]).keys():
#         print(table)

provider = 'upya_test'
# for table in providers[provider].get('tables',[]).keys():
for table in ['applications']:
    tablesync = TableInterface(provider,table)
    tablesync.fetchAndUploadProviderData()
    # tablesync.syncdbtable()
