"""
This script connects to an AWS Redshift cluster and performs the following:
1. Drops existing tables if they exist.
2. Creates new staging and analytics tables for the Sparkify data warehouse project.
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops existing tables in the Redshift database.

    Parameters:
    cur -- cursor object to execute SQL commands
    conn -- Redshift connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print("Table dropped.")

def create_tables(cur, conn):
    """
    Creates staging and final star-schema tables in the Redshift database.

    Parameters:
    cur -- cursor object to execute SQL commands
    conn -- Redshift connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print("Table created.")

def main():
    """
    Main function to:
    - Read configuration
    - Connect to Redshift
    - Drop and recreate tables
    - Close the database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
