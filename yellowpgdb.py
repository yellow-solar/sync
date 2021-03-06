"""
Function to connect the a PostgreSQL database depending on live environment and return DB object
"""

import psycopg2, config, logging
from sqlalchemy import create_engine

class yellowpgdb:
    """ Class for using the Yellow Postgres DB """
    def __init__(self):
        # Get db config
        self.cfg = config.config(section = "yellowpgdb")

    def get_engine(self, echo=False):
        engine = create_engine(
            "postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}".format(
                host=self.cfg['host'],
                port=self.cfg['port'],
                db=self.cfg['db'],
                user=self.cfg['user'],
                pw=self.cfg['password']
            ), echo = echo
        )
        return(engine)

    def connect(self):
        # Try open connection
        try:
            conn = psycopg2.connect(host=self.cfg['host'],
                                port=self.cfg['port'],
                                database=self.cfg['db'],
                                user=self.cfg['user'],
                                password=self.cfg['password'],
                    )
            return(conn)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)       
            
    def get_version(self):
        conn = psycopg2.connect(host=self.cfg['host'],
                                port=self.cfg['port'],
                                database=self.cfg['db'],
                                user=self.cfg['user'],
                                password=self.cfg['password'])
        # get version
        cur = conn.cursor()
        cur.execute('SELECT version()')
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print('PostgreSQL database version: ' + ", ".join(db_version))
        # close the communication with the PostgreSQL
        cur.close()        
    
if __name__ == "__main__":
    pgdb = yellowpgdb()
    conn = pgdb.connect()
    if conn is not None:
        conn.close()
    # engine = pgdb.get_engine()
    # x = engine.connect()
    # y = x.execute("select * from core.products;")
    # print([v for v in y])