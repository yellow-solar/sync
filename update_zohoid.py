import pandas as pd
from yellowpgdb import yellowpgdb

accounts = pd.read_csv('IDupdates/account_ids_to_update.csv',dtype = str,)[['account_id','account_external_id','ID']]
apps = pd.read_csv('IDupdates/app_ids_to_update.csv',dtype = str,)[['application_id','application_external_id','ID']]

db = yellowpgdb()
conn = db.connect()

# Accounts
with conn.cursor() as cursor:
    for i in accounts.index:
        print(accounts.loc[i])
        row = accounts.loc[i]
        sql = f"update core.accounts set zoho_id = {row['ID']} where account_external_id = '{row['account_external_id']}'"
        print(sql)
        cursor.execute(sql)     

# Applications
# with conn.cursor() as cursor:
#     for i in apps.index:
#         print(apps.loc[i])
#         row = apps.loc[i]
#         sql = f"update core.applications set zoho_id = {row['ID']} where application_external_id = '{row['application_external_id']}'"
#         print(sql)
#         cursor.execute(sql)     

conn.commit()
conn.close()
