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
CORE_SCHEMA = 'core'

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

def createUpdate(row):
    dtype = row['type']
    col = row[ORG]
    select = f"{col} = b.{col}"
    if (dtype not in ('TEXT','GEOGRAPHY')):  
        select = f"{col} = cast(b.{col} as {dtype})"
    elif (dtype == 'GEOGRAPHY'):
        select = f"{col} = ST_GeographyFromText('POINT('||b.{col}||')')"
    return(select)

# 1. execute the insert casts
source_mapping['select'] = source_mapping.apply(createSelect, axis = 1)
casts = ', '.join(source_mapping['select'].values.tolist())
# generate a select statement
select = f"SELECT {casts} FROM {PROVIDER}.{TABLE} a"
cols_csv = ", ".join(source_mapping[ORG].values.tolist())
insert = f"INSERT INTO {CORE_SCHEMA}.{TABLE} ({cols_csv})"
where = f"WHERE a.{update_on} NOT IN (select c.{update_on} FROM {CORE_SCHEMA}.{TABLE} c)"
insert_sql= "\n".join([insert, select, where])

# 2. update 
update = f"UPDATE {CORE_SCHEMA}.{TABLE} a"
source_mapping['update'] = source_mapping.apply(createUpdate, axis = 1)
casts = ', '.join(source_mapping['update'].values.tolist()+["change_timestamp = CURRENT_TIMESTAMP"])
sets = f"SET {casts} FROM {PROVIDER}.{TABLE} b"
where = f"WHERE a.{update_on} = b.{update_on}"
update_sql = "\n".join([update, sets, where])


# Yellowdb connections and executions
db = yellowpgdb()
db_conn = db.connect()
cursor = db_conn.cursor()
cursor.execute(insert_sql)
db_conn.commit()
cursor.execute(update_sql)
db_conn.commit()
