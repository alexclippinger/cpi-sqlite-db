# CPI-SQLite Database

The purpose of this project is to load data from the BLS website, which is regularly updated in the form of text files, to a simple SQLite database. 

## Data

The data extracted is CPI data from the BLS.gov website, specifically from the "All Urban Consumers" database [(CPI-U)](https://www.bls.gov/cpi/data.htm). The CPI-U data series covers approximately 93 percent of the total population of the United States and includes data for the U.S., four Census regions, nine Census divisions, two sizes of city classes, eight cross-classifications of regions and size-classes, and 23 local areas. 

For more information on the CPI Survey see here: https://download.bls.gov/pub/time.series/cu/cu.txt

## Repository Details

The following outlines the files and processes contained in this repository:

1. `create_db.py`: Creates an SQLite database with table schemas. 

2. `insert_tables.py`: Extracts CPI-U data from the BLS website and inserts into the SQLite database.

3. `.github/workflows/actions.yml`: Runs the `insert_tables.py` script weekly to update the database if new data is added to the series.

## Series ID Format

The data table can be joined to the other tables to get more information on period, item codes, and area codes. The data is joined on elements of the "series_id" column, which are included as separate columns in the SQLite database. The elements of the "series_id" column are included in the table below:

| Position | Value | Field Name                     |
|----------|-------|--------------------------------|
| 1-2      | CU    | Prefix                         |
| 3        | U     | (Not) Seasonal Adjustment Code |
| 4        | R     | Periodicity Code               |
| 5-8      | 0000  | Area Code                      |
| 9        | S     | Base Code                      |
| 10-16    | A0L1E | Item Code                      |
