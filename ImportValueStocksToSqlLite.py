import pandas as pd
import sqlite3

# Paths to files
csv_file_path = '20250112-130626-3.DLEVEL_ADVANCED_INFO.CSV'
db_file_path = 'ValueStocksDB.db'

# Load the CSV data
csv_data = pd.read_csv(csv_file_path)

# Connect to the SQLite database
conn = sqlite3.connect(db_file_path)
cursor = conn.cursor()

# Insert DATE_ID into VS_META_IMPORTDATE and fetch the ID for use in VS_IMPORT table
for index, row in csv_data.iterrows():
    print(row)
    datenum = row['DATENUM']
    date = row['DATE']

    # Insert the date into VS_META_IMPORTDATE
    cursor.execute(
        """
        INSERT OR IGNORE INTO VS_META_IMPORTDATE (DATENUM, DATE)
        VALUES (?, ?)
        """,
        (datenum, date)
    )

    # Fetch the DATE_ID for the inserted/updated date
    cursor.execute("SELECT ID FROM VS_META_IMPORTDATE WHERE DATENUM = ?", (datenum,))
    date_id = cursor.fetchone()[0]

    # Handle SYMBOL and COMPANY_NAME
    symbol = row['SYMBOL']
    company_name = row['NAME']

    # Insert into VS_META_STOCKINFO if not exists
    cursor.execute(
        """
        INSERT OR IGNORE INTO VS_META_STOCKINFO (SYMBOL_ID, NAME)
        VALUES (?, ?)
        """,
        (symbol, company_name)
    )

    # Fetch the STOCK_ID for the inserted/updated stock info
    cursor.execute("SELECT ID FROM VS_META_STOCKINFO WHERE SYMBOL_ID = ?", (symbol,))
    stock_id = cursor.fetchone()[0]

    # Handle SECTOR
    sector = row['SECTOR']

    # Insert into VS_META_SECTOR if not exists
    cursor.execute(
        """
        INSERT OR IGNORE INTO VS_META_SECTOR (SECTOR_NAME)
        VALUES (?)
        """,
        (sector,)
    )

    # Fetch the SECTOR_ID for the inserted/updated sector
    cursor.execute("SELECT ID FROM VS_META_SECTOR WHERE SECTOR_NAME = ?", (sector,))
    sector_id = cursor.fetchone()[0]

    # Handle VALUATION
    valuation = row['VALUATION']

    # Insert into VS_META_VALUATION if not exists
    cursor.execute(
        """
        INSERT OR IGNORE INTO VS_META_VALUATION (VALUATION)
        VALUES (?)
        """,
        (valuation,)
    )

    # Fetch the VALUATION_ID for the inserted/updated valuation
    cursor.execute("SELECT ID FROM VS_META_VALUATION WHERE VALUATION = ?", (valuation,))
    valuation_id = cursor.fetchone()[0]

    # Handle MKCAPTYPE
    mkcaptype = row['MKCAPTYPE']

    # Insert into VS_META_MARKETCAPTYPE if not exists
    cursor.execute(
        """
        INSERT OR IGNORE INTO VS_META_MARKETCAPTYPE (MARKETCAPTYPE)
        VALUES (?)
        """,
        (mkcaptype,)
    )

    # Fetch the MKCAPTYPE_ID for the inserted/updated market cap type
    cursor.execute("SELECT ID FROM VS_META_MARKETCAPTYPE WHERE MARKETCAPTYPE = ?", (mkcaptype,))
    mkcaptype_id = cursor.fetchone()[0]

	# Handle TREND
    trend = row['TREND']

    # Insert into VS_META_TREND if not exists
    cursor.execute(
        """
        INSERT OR IGNORE INTO VS_META_TREND (TREND)
        VALUES (?)
        """,
        (trend,)
    )

    # Fetch the TREND_ID for the inserted/updated trend
    cursor.execute("SELECT ID FROM VS_META_TREND WHERE TREND = ?", (trend,))
    trend_id = cursor.fetchone()[0]

    # Handle FUNDAMENTAL
    fundamental = row['FUNDAMENTAL']

    # Insert into VS_META_FUNDAMENTAL if not exists
    cursor.execute(
        """
        INSERT OR IGNORE INTO VS_META_FUNDAMENTAL (FUNDAMENTAL)
        VALUES (?)
        """,
        (fundamental,)
    )

    # Fetch the FUNDAMENTAL_ID for the inserted/updated fundamental
    cursor.execute("SELECT ID FROM VS_META_FUNDAMENTAL WHERE FUNDAMENTAL = ?", (fundamental,))
    fundamental_id = cursor.fetchone()[0]

    # Handle MOMENTUM
    momentum = row['MOMENTUM']

    # Insert into VS_META_MOMEMTUM if not exists
    cursor.execute(
        """
        INSERT OR IGNORE INTO VS_META_MOMEMTUM (MOMEMTUM)
        VALUES (?)
        """,
        (momentum,)
    )

    # Fetch the MOMEMTUM_ID for the inserted/updated momentum
    cursor.execute("SELECT ID FROM VS_META_MOMEMTUM WHERE MOMEMTUM = ?", (momentum,))
    momentum_id = cursor.fetchone()[0]

    # Insert data into VS_IMPORT table
    cursor.execute(
        """
        INSERT INTO VS_IMPORT (IMPORT_DATE_ID,SYMBOL_ID,SECTOR_ID,CMP,VALUATION_ID,
                          FAIR_RANGE,PE,SECTOR_PE,MARKET_CAP,MARKETCAPTYPEID,TREND_ID,
                          FUNDAMENTAL_ID,MOMEMTUM_ID,DERATIO,PRICETOSALES,PLEDGE,QBS,
                          [QBS%],AGS,[AGS%],VALUATION_DCF,VALUATION_GRAHAM,VALUATION_EARNING,VALUATION_BOOKVALUE,VALUATION_SALES)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            date_id,
            stock_id,
            sector_id,
            row['CMP'],
            valuation_id,
            row['FAIRRANGE'],
            row['PE'],
            row['SECTORPE'],
            row['MARKETCAP'],
            mkcaptype_id,
            trend_id,
            fundamental_id,
            momentum_id,
            row['DERATIO'],
            row['PRICETOSALES'],
            row['PLEDGE'],
            row['QBS'],
            row['QBS%'],
            row['AGS'],
            row['AGS%'],
            row['VALUATION_DCF'],
            row['VALUATION_GRAHAM'],
            row['VALUATION_EARNING'],
            row['VALUATION_BOOKVALUE'],
            row['VALUATION_SALES']
        )
    )

# Commit the transaction and close the connection
conn.commit()
conn.close()

print("CSV data successfully imported into VS_IMPORT table using the existing schema.")
