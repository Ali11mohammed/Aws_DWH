import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Drop existing tables to avoid duplication/conflicts during re-runs
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print("Table dropped.")

# Create staging and final tables using SQL queries from sql_queries.py
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print("Table created.")


def main():
    # Read AWS and Redshift configuration from dwh.cfg
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Connect to Redshift cluster using credentials from config
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
     # Drop and recreate all tables
    drop_tables(cur, conn)
    create_tables(cur, conn)
    # Close connection to the database
    conn.close()


if __name__ == "__main__":
    main()