""" Program to get snapshot, upload to PostgresDB, and then rug SQL functions to update core tables  """

# System
from io import StringIO

# Third party libraries
import pandas as pd
import numpy as np
from datetime import datetime

# Connection modules
from APIconn import APIconn
from yellowpgdb import yellowpgdb
from mapping import headerMap

print("-----------------------------------------------------")
print("Current Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

TABLE = 'accounts'
PROVIDER = 'angaza'
ORG = 'yellow'
source_mapping = headerMap('accounts', 'angaza',sys=False)

# provider connection and snapshots
print("Fetching provider file...")
apiconn = APIconn(PROVIDER)
snapshot = apiconn.pullSnapshot(TABLE)

# process file
print("Processing file...")
lines = StringIO(snapshot)
df = (pd.read_csv(lines, dtype = str, 
                    na_values=['None','none','NONE',""])
        .replace('[\\r\\n<>&\+]','',regex=True) 
        .replace(np.nan,'\\N')
    )
df = df[source_mapping[PROVIDER].values.tolist()]
df.columns = source_mapping[ORG].values.tolist()
df = df.dropna(axis = 1, how = 'all')

# add new fields
df['external_sys'] = PROVIDER

# Process location from lat long to long lat (for PostGIS)
geo_cols = source_mapping.loc[source_mapping['type']=='GEOGRAPHY',ORG]
for col in geo_cols:
    df[col] = df[col].apply(lambda x: " ".join(x.split(',')[::-1]))

# Re-create table header in case different
print("Re-creating table header...")
db = yellowpgdb()
db_engine = db.get_engine()
df.head(0).to_sql(
    TABLE, db_engine,
    schema = PROVIDER, 
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
cur.copy_from(lines, f"{PROVIDER}.{TABLE}") 
db_conn.commit()

print("Current Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("-----------------------------------------------------")

# conn = APIconn('upya')
# snapshot = conn.pullSnapshot('users')
# print(len(snapshot))
# print(snapshot)