import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop any existing tables from sparkify DB
       #cur -- cursor to connected DB. Enables SQL statements execution
       #conn -- (psycopg2) connection to Postgres database (sparkify DB).
       
       
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Can't drop table: " + query)
            print(e)
    print("tables dropped successfully.")


def create_tables(cur, conn):
    """Drop any existing tables from sparkify DB
       #cur -- cursor to connected DB. Enables SQL statements execution
       #conn -- (psycopg2) connection to Postgres database (sparkify DB)."""
    
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Can't create table: " + query)
            print(e)
    print("Tables created successfully.")


def main():
    """Connect to AWS Redshift, create new DB (sparkifydb),
       drop any existing tables, create new tables. Close DB connection"""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()