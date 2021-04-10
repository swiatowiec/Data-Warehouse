import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from files stored in S3 to the staging tables based on queries declared on the script 'sql_queries.py'
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    select and transform data from staging tables into the star schema (dimensional and fact tables) using the declared on the script 'sql_queries.py'
    """
    for query in insert_table_queries:
        print('query:', query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Extract metadata from S3, transform it using a staging table, and load it into dimensional tables for analysis
    """
    print('start')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('after connection')
    
    load_staging_tables(cur, conn)
    print('after staging tables')
    insert_tables(cur, conn)
    print('after inserting')

    conn.close()


if __name__ == "__main__":
    main()