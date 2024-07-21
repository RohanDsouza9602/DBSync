import pymysql
import psycopg2
from psycopg2.extras import DictCursor


def fetch_mysql_data(conn, table_name, columns):
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        columns_str = ', '.join(columns)
        query = f"SELECT {columns_str} FROM {table_name}"
        cursor.execute(query)
        data = cursor.fetchall()
        return data
    except Exception as e:
        print(f"Error fetching data from MySQL: {e}")
        return []


def fetch_postgres_data(conn, table_name, columns):
    try:
        cursor = conn.cursor(cursor_factory=DictCursor)
        columns_str = ', '.join(columns)
        query = f"SELECT {columns_str} FROM {table_name}"
        cursor.execute(query)
        data = cursor.fetchall()
        return data
    except Exception as e:
        print(f"Error fetching data from PostgreSQL: {e}")
        return []


def fetch_data(conn, query, db_type):
    if db_type == 'mysql':
        cursor = conn.cursor(pymysql.cursors.DictCursor)
    elif db_type == 'postgres':
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    else:
        raise ValueError("Unsupported database type")

    cursor.execute(query)
    return cursor.fetchall()

def fetch_db_data(conn, table_name, columns, db_type):
    try:
        if db_type == "MySQL":
            cursor = conn.cursor(pymysql.cursors.DictCursor)
        if db_type == "PostgreSQL":
            cursor = conn.cursor(cursor_factory=DictCursor)
        columns_str = ', '.join(columns)
        query = f"SELECT {columns_str} FROM {table_name}"
        cursor.execute(query)
        data = cursor.fetchall()
        return data
    except Exception as e:
        print(f"Error fetching data from PostgreSQL: {e}")
        return []
