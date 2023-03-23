#!/usr/bin/python

import os
import sqlite3
import pandas as pd


def main(db_file):
    example_query = """
        SELECT * 
        FROM data_view
        WHERE 
            area_code = '0000' AND 
            item_code = 'SA0'
    """
    conn = sqlite3.connect(db_file)
    
    df = pd.read_sql_query(example_query, conn)
    print(f"Queried {len(df.index)} records.")

    conn.close()


if __name__ == '__main__':
    db_file = 'cpi-u.db' 
    main(db_file)