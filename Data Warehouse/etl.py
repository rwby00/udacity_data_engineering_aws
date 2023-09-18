import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Loads data from S3 buckets into staging tables in the Redshift database.
    
    Parameters:
        - cur: psycopg2 cursor object. The cursor object to execute the COPY commands.
        - conn: psycopg2 connection object. The connection to the database instance.
        
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Processes the data in the staging tables and inserts it into the analytics tables.
    
    Parameters:
        - cur: psycopg2 cursor object. The cursor object to execute the INSERT INTO commands.
        - conn: psycopg2 connection object. The connection to the database instance.
        
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Establishes connection with the Redshift database and gets cursor to it.
    - Loads data from S3 buckets into staging tables.
    - Processes the data in the staging tables and inserts it into the analytics tables.
    - Finally, closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
