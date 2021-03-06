""" tutorial using psycopg2 """

from yellowpgdb import yellowpgdb
from pprint import pprint

def create_schemas():
    commands = [
        "CREATE SCHEMA IF NOT EXISTS core;",
        "CREATE SCHEMA IF NOT EXISTS angaza;",
        "CREATE SCHEMA IF NOT EXISTS upya;",
        "CREATE SCHEMA IF NOT EXISTS upya_test;",
        "CREATE SCHEMA IF NOT EXISTS upya_uganda;",
    ]
    pgdb = yellowpgdb()
    conn = pgdb.connect()
    cur = conn.cursor()
    # create table one by one
    for command in commands:
        cur.execute(command)
    # close the cursor
    cur.close()
    # commit the changes
    conn.commit()
    # print results and close the connection
    pprint(conn.notices)
    conn.close()

create_schemas()
