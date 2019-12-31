""" Program to get snapshot, upload to PostgresDB, and then rug SQL functions to update core tables  """

# System
from io import StringIO

# Third party libraries
import pandas as pd

# Connection modules
from APIconn import APIconn
from yellowpgdb import yellowpgdb

TABLE = 'accounts'
PROVIDER = 'angaza'

# provider connection and snapshots
apiconn = APIconn(PROVIDER)
snapshot = apiconn.pullSnapshot(TABLE)
# print(snapshot)

# process file
lines = StringIO(snapshot)
df = pd.read_csv(lines, dtype = str, 
                na_values=['None','none','NONE'])


# yellowdb connections
db = yellowpgdb()
db_engine = db.get_engine(echo=True)

# Re-create table header in case different
df.head(0).to_sql(
    TABLE, db_engine,schema = PROVIDER, if_exists='replace',index=False)

# Create stringIO
lines = StringIO()
df.to_csv(lines, sep='\t', header=False, index=False)
lines.seek(0)

# Upload contents
db_conn = db_engine.raw_connection()
cur = db_conn.cursor()
cur.copy_from(lines, f"{PROVIDER}.{TABLE}") 
db_conn.commit()

# conn = APIconn('upya')
# snapshot = conn.pullSnapshot('users')
# print(len(snapshot))
# print(snapshot)