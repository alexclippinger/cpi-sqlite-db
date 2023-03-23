#!/usr/bin/python

import sqlite3
import os


def create_sqlite_connection(db_file: str) -> sqlite3.Connection:
    """Creates a database connection to a SQLite database.

    Parameters:
        db_file (str): The name of the database file to connect to.

    Returns:
        conn (sqlite3.Connection): A connection object representing the database. If the database file specified by `db_file` does not exist, a new file will be created.      
    """
    conn = None
    try: 
        conn = sqlite3.connect(db_file)
        print('Opened database successfully')
        return conn
    except sqlite3.Error as e:
        print(e)


def create_table(conn: sqlite3.Connection, query: str) -> None:
    """Creates a table in the specified SQLite database connection.

    Parameters:
        conn (sqlite3.Connection): The database connection object to create the table in.
        query (str): The SQL query string to create the table.

    Returns:
        None

    Notes:
        This function executes the SQL query string `query` to create a new table in the database
        specified by the `conn` parameter. If the table already exists, the query is ignored and
        no error is raised. If there is an error creating the table, an exception is raised with
        an error message describing the cause of the error.
    """
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(query)
            print('Table has been created in the database')
    except Exception as e:
        print(f'Error: {e}')


def main(db_file):
    create_data_query = f""" 
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER NOT NULL PRIMARY KEY ON CONFLICT IGNORE,
            series_id TEXT,
            prefix TEXT,
            seasonal TEXT,
            periodicity TEXT,
            area_code TEXT,
            item_code TEXT,
            year INTEGER,
            period TEXT,
            value REAL,
            footnote_codes TEXT, 
            FOREIGN KEY (area_code) REFERENCES areas(area_code),
            FOREIGN KEY (item_code) REFERENCES items(item_code), 
            FOREIGN KEY (period) REFERENCES periods(period),
            UNIQUE(series_id, year, period)
        )
    """

    create_periods_query = f""" 
        CREATE TABLE IF NOT EXISTS periods (
            period TEXT NOT NULL PRIMARY KEY,
            period_abbr TEXT,
            period_name TEXT
        )
    """

    create_items_query = f""" 
        CREATE TABLE IF NOT EXISTS items (
            item_code TEXT NOT NULL PRIMARY KEY,
            item_name TEXT,
            display_level INTEGER,
            selectable TEXT,
            sort_sequence INTEGER
        )
    """

    create_areas_query = f""" 
        CREATE TABLE IF NOT EXISTS areas (
            area_code TEXT NOT NULL PRIMARY KEY,
            area_name TEXT,
            display_level INTEGER,
            selectable TEXT,
            sort_sequence INTEGER
        )
    """

    conn = create_sqlite_connection(db_file)
    if conn:
        create_table(conn=conn, query=create_data_query)
        create_table(conn=conn, query=create_items_query)
        create_table(conn=conn, query=create_periods_query)
        create_table(conn=conn, query=create_areas_query)
        conn.close()


if __name__ == '__main__':
    db_file = 'cpi-u.db'
    main(db_file)
    