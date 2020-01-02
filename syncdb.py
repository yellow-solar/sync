""" Program to get snapshot, upload to PostgresDB, and then rug SQL functions to update core tables  """

# Third party libraries
import pandas as pd
from datetime import datetime

# Connection modules
from APIconn import APIconn
from yellowpgdb import yellowpgdb
from mapping import headerMap

print("Current Time:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

TABLE = 'accounts'
PROVIDER = 'angaza'
ORG = 'yellow'
source_mapping = headerMap('accounts', 'angaza',sys=True)
update_on = source_mapping.loc[source_mapping['update_on']==1,ORG].iloc[0]

def createSelect(row):
    dtype = row['type']
    col = row[ORG]
    select = f"a.{col}"
    if (dtype not in ('TEXT','GEOGRAPHY')):  
        select = f"cast(a.{col} as {dtype}) as {col}"
    elif (dtype == 'GEOGRAPHY'):
        select = f"ST_GeographyFromText('POINT('||{col}||')') as {col}"
    return(select)

# execute the insert casts
source_mapping['select'] = source_mapping.apply(createSelect, axis = 1)
casts = ', '.join(source_mapping['select'].values.tolist())
# generate a select statement
select = f"select {casts} from {PROVIDER}.{TABLE} a"
cols_csv = ", ".join(source_mapping[ORG].values.tolist())
insert = f"insert into core.{TABLE} ({cols_csv})"
where = f"where a.{update_on} not in (select c.{update_on} from core.{TABLE})"
insert_not_update = "\n".join([insert, select, where, " limit 5;"])

# Yellowdb connections
db = yellowpgdb()
db_conn = db.connect()
cursor = db_conn.cursor()

# execute the update
# cursor.execute(insert_all)
print(insert_not_update)
