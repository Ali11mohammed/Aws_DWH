"""
ETL script that loads raw JSON data from Amazon S3 into Redshift.
It first loads data into staging tables using COPY, then inserts processed records
into star schema analytics tables.
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Load data from S3 into Redshift staging tables using COPY commands.

    Parameters:
    cur -- cursor object to execute queries
    conn -- Redshift database connection object
    """
    print("🚚 Starting COPY to staging tables...")
    for query in copy_table_queries:
        print(f"⏳ Running COPY: {query[:40]}...")
        cur.execute(query)
        conn.commit()
        print("✅ COPY finished.\n")

def insert_tables(cur, conn):
    """
    Insert transformed data from staging tables into final analytical tables.

    Parameters:
    cur -- cursor object to execute queries
    conn -- Redshift database connection object
    """
    print("📤 Starting INSERT from staging to final tables...")
    for query in insert_table_queries:
        print(f"⏳ Running INSERT: {query[:40]}...")
        cur.execute(query)
        conn.commit()
        print("✅ INSERT finished.\n")

def main():
    """
    Main function that performs the ETL process:
    - Loads configuration from dwh.cfg
    - Connects to Redshift
    - Executes loading into staging tables
    - Executes insertion into final tables
    - Closes the connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("🔗 Connecting to Redshift cluster...")
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()
    print("✅ Connected.\n")

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    print("✅ Connection closed. ETL process complete.")

if __name__ == "__main__":
    main()
