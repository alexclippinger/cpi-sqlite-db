#!/usr/bin/python

import sqlite3
import urllib.request
import pandas as pd
from io import StringIO

def read_text_from_url(url: str) -> str:
    """ 
    Read text data from a provided URL.

    Parameters:
        url (str): URL of the text file containing the data.

    Returns:
        str: The function returns a string of text data if it is successfully read from the URL. If there is an error, the function returns None.
    """
    try:
        with urllib.request.urlopen(url) as response:
            data = response.read()
            text = data.decode('utf-8')
            return text
    except Exception as e:
        print(f'Error: {e}')
        return None

def text_to_df(text: str, separator: str = '\t') -> pd.DataFrame:
    """ 
    Convert text data to a pandas DataFrame object.

    Parameters:
        text (str): Text data to be converted to a DataFrame.
        separator (str): Separator character used in the text data.

    Returns:
        pandas.DataFrame: The function returns a pandas DataFrame object if it successfully converts the text data. If there is an error, the function returns None.
    """
    try:
        df = pd.read_csv(StringIO(text), sep=separator)
        df = df.rename(columns=lambda x: x.strip())
        return df
    except Exception as e:
        print(f'Error: {e}')
        return None

def get_data_cols(data: pd.DataFrame) -> pd.DataFrame:
    """Breaks down the series_id column of the data table into codes that can be used to join to other tables."""
    data['id'] = data.index + 1
    data['prefix'] = data['series_id'].apply(lambda x: x[:2])
    data['seasonal'] = data['series_id'].apply(lambda x: x[2:3])
    data['periodicity'] = data['series_id'].apply(lambda x: x[3:4])
    data['area_code'] = data['series_id'].apply(lambda x: x[4:8])
    data['item_code'] = data['series_id'].apply(lambda x: x[8:].strip())
    return data

def arrange_data_cols(data: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Re-arranges columns using a list of column names"""
    return data[cols]

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

def insert_non_data_table(conn: sqlite3.Connection, url: str, table_name: str, separator: str) -> None:
    """ 
    Insert data from a text file located at the provided URL to a SQLite database table.

    Parameters:
        conn (sqlite3.Connection): Connection object to the SQLite database.
        url (str): URL of the text file containing the data.
        table_name (str): Name of the SQLite database table to insert the data.
        data_cols (list): List of column names to select from the data file.
        separator (str): Separator character used in the data file.

    Returns:
        Inserts data to the specified SQLite database table and prints the number of records inserted to the console.
    """
    try:
        text = read_text_from_url(url)
        if text:
            rows = text.strip().split('\n')
            rows = [row.split(separator) for row in rows[1:]]
            
            with conn:
                cursor = conn.cursor()
                
                insert_query = f""" 
                    INSERT OR IGNORE INTO {table_name} VALUES ({','.join('?' * len(rows[0]))})
                """
                cursor.executemany(insert_query, rows)
                print(f"Inserted {cursor.rowcount} records to the table.")
        else:
            print("Unable to read the text file from URL.")
    except Exception as e:
        print(f'Error: {e}')

def insert_data_table(conn: sqlite3.Connection, url: str, table_name: str, data_cols: list, separator: str) -> None: 
    """
    Insert data from a text file located at the provided URL to a SQLite database table.

    Parameters:
        conn (sqlite3.Connection): Connection object to the SQLite database.
        url (str): URL of the text file containing the data.
        table_name (str): Name of the SQLite database table to insert the data.
        data_cols (list): List of column names to select from the data file.
        separator (str): Separator character used in the data file.

    Returns:
        Inserts data to the specified SQLite database table.
    """
    try:
        text = read_text_from_url(url)
        if text:
            data = text_to_df(text, separator=separator)
            data = get_data_cols(data)
            data = arrange_data_cols(data, data_cols)
            data.to_sql(table_name, conn, if_exists='append', index=False)
        else:
            print("Unable to read text file from URL.")
    except Exception as e:
        print(f'Error: {e}')


def create_view(conn: sqlite3.Connection) -> None:
    """Creates a custom view using the 4 existing tables."""
    view_query = """
        CREATE VIEW data_view AS 
            SELECT 
                d.series_id, d.area_code, a.area_name, d.item_code, 
                i.item_name, d.year, d.period, p.period_name, d.value
            FROM data d
            LEFT JOIN areas a ON 
                a.area_code = d.area_code
            LEFT JOIN items i ON 
                i.item_code = d.item_code
            LEFT JOIN periods p ON
                p.period = d.period
    """
    with conn:
        cursor = conn.cursor() 
        cursor.execute("DROP VIEW IF EXISTS data_view")
        cursor.execute(view_query)
        cursor.execute("SELECT COUNT(*) FROM data_view")
        rows = cursor.fetchone()[0]
        print(f"Created view with {rows} records.")


def update_db(db_file):
    base_url = 'https://download.bls.gov/pub/time.series/cu/'
    area_codes = 'cu.area'
    item_codes = 'cu.item'
    period_codes = 'cu.period'
    data_file = 'cu.data.0.Current'
    data_cols = ['id', 'series_id', 'prefix', 'seasonal', 'periodicity', 'area_code', 'item_code', 'year', 'period', 'value', 'footnote_codes']

    conn = create_sqlite_connection(db_file)
    if conn:
        insert_non_data_table(conn=conn, url=base_url+area_codes, table_name='areas', separator='\t')
        insert_non_data_table(conn=conn, url=base_url+period_codes, table_name='periods', separator='\t')
        insert_non_data_table(conn=conn, url=base_url+item_codes, table_name='items', separator='\t')
        insert_data_table(conn=conn, url=base_url+data_file, table_name='data', data_cols=data_cols, separator='\t')
        create_view(conn)
        conn.close()


if __name__ == '__main__':
    update_db(os.environ['DATABASE_URL'])
    
