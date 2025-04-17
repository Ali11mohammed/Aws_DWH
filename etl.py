import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Load raw data from S3 into Redshift staging tables using COPY command
def load_staging_tables(cur, conn):
    print("🚚 Starting COPY to staging tables...")
    for query in copy_table_queries:
        print(f"⏳ Running COPY: {query[:40]}...")  
        cur.execute(query)
        conn.commit()
        print("✅ COPY finished.\n")

# Insert transformed data from staging tables into the final star schema tables
def insert_tables(cur, conn):
    print("📤 Starting INSERT from staging to final tables...")
    for query in insert_table_queries:
        print(f"⏳ Running INSERT: {query[:40]}...")
        cur.execute(query)
        conn.commit()
        print("✅ INSERT finished.\n")

def main():
    # Load configuration for Redshift connection and S3 paths
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Execute ETL steps: COPY to staging, then INSERT to final tables
    print("🔗 Connecting to Redshift cluster...")
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    # Close the connection when done
    cur = conn.cursor()
    print("✅ Connected.\n")

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    print("✅ Connection closed. ETL process complete.")
    
if __name__ == "__main__":
    main()
