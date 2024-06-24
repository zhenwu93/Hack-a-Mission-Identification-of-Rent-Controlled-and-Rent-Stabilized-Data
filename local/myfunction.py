import json
import sys
import psycopg2
from config import load_config
from pathlib import Path
import pandas as pd
import tarfile
import urllib.request
from enum import Enum
import pypdf
import csv
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger
from psql import Base, db, session

# globals
parts = []
rows_with_extra_elements = []
all_data_to_insert = []
RENT_STAB_BLDG_LISTINGS = 'rentstabbldglistings'

class BoroughIdentifiers(Enum):
    MANHATTAN = ('1', './rent-stab-pdfs/2022-DHCR-Manhattan.pdf')
    BRONX = ('2', './rent-stab-pdfs/2022-DHCR-Bronx.pdf')
    BROOKLYN = ('3', './rent-stab-pdfs/2022-DHCR-Brooklyn.pdf')
    QUEENS = ('4', './rent-stab-pdfs/2022-DHCR-Queens.pdf')
    STATEN_ISLAND = ('5', './rent-stab-pdfs/2022-DHCR-Staten-Island.pdf')

class RentStabFeatures(Enum):
    zip = 25.0
    bldgno1 = 75.0
    street1 = 185.0
    stsufx1 = 312.0
    bldgno2 = 372.0
    street2 = 451.0
    stsufx2 = 567.0
    city = 619.0
    county = 702.0
    status1 = 769.0
    status2 = 879.0
    status3 = 980.0
    block = 1079.0
    lot = 1143.0
    boroughid = 2024.0

names_as_appear_in_pdf = ['ZIP', 'BLDGNO1', 'STREET1', 'STSUFX1', 'BLDGNO2', 'STREET2', 'STSUFX2', 'CITY', 'COUNTY', 'STATUS1', 'STATUS2', 'STATUS3', 'BLOCK', 'LOT']

def visitor_body(text, cm, tm, font_dict, font_size):
    """Visitor function for extracting text from the body of the PDF"""
    y = tm[5]
    x = tm[4]
    if y > 25 and y < 865 and text and text not in names_as_appear_in_pdf:
        parts.append((text, x, y))

def split_data(data):
    """Split the data into arrays each time it encounters the LOT feature"""
    result = []
    current_array = []
    for item in data:
        current_array.append(item)
        if item[1] == RentStabFeatures.lot.value:
            result.append(current_array)
            current_array = []
    return result

def fill_empty_features(data, borough_id):
    """Fill the empty features in each sample with None values. Add the borough_id to each sample."""
    for array in data:
        for feature in RentStabFeatures:
            if (feature == RentStabFeatures.boroughid):
                array.append((borough_id, feature.value))
            if not any(feature.value == item[1] for item in array):
                array.append((None, feature.value))
        array.sort(key=lambda x: x[1])
    return data

def remove_extra_elements(data):
    """Separate the arrays that contain more than the number of features in RentStabFeatures enum.
    These arrays will be further split into separate arrays using a different process than the one used in split_data."""
    result = []
    for array in data:
        if len(array) > len(RentStabFeatures):
            rows_with_extra_elements.append(array)
        else:
            result.append(array)
    return result

def remove_tuple_data(data):
    """Clear the tuple data that has been used to keep track of the samples position in the PDF."""
    return [[item[0] for item in array] for array in data]

def get_arr_of_rent_stab_data_rows(pdf_name, visitor_function, borough_id):
    assert len(parts) == 0
    with open(pdf_name, 'rb') as file:
        pdf_reader = pypdf.PdfReader(file)
        num_pages = len(pdf_reader.pages)

        for page_num in range(num_pages):
            page = pdf_reader.pages[page_num]
            page.extract_text(visitor_text=visitor_function)

        return remove_tuple_data(remove_extra_elements(fill_empty_features(split_data(parts), borough_id)))

def create_dict(data):
    """Hard to explain ATM, will update later."""
    result = {}
    for i, row in enumerate(data):
        key = 'fnma' + str(i)
        result[key] = row
    return result

def split_rows_with_extra_elements(data):
    """Performs parsing specific to the rows that have more elements than the RentStabFeatures enum.
    This situation occurs when there are rows in the data that are missing the LOT feature."""
    result = {}
    for item in data:
        # shed the borough_id and the empty cells
        if len(item) < 3:
            continue
        # if the key is not in the result dictionary, add it
        elif item[2] not in result:
            result[item[2]] = []
        # for items that have all 3 tuple elements, append their value and feature coordinates to the result dictionary
        if not len(item) < 3:
            result[item[2]].append((item[0], item[1]))
    return [result[key] for key in result]

def add_borough_data_to_arr(borough_id, file_name):
    global all_data_to_insert
    print(borough_id)
    print(file_name)
    borough_arr = get_arr_of_rent_stab_data_rows(file_name, visitor_body, borough_id)
    outlier_dict = create_dict(rows_with_extra_elements)
    split_borough_outliers = []
    for key in outlier_dict:
      split_borough_outliers += split_rows_with_extra_elements(outlier_dict[key])
    outlier_borough_arr = remove_tuple_data(fill_empty_features(split_borough_outliers, borough_id))
    all_data_to_insert += borough_arr
    all_data_to_insert += outlier_borough_arr

def clean_up_global_vars():
    parts.clear()
    rows_with_extra_elements.clear()

def parse_five_boroughs_pdfs():
    for borough in BoroughIdentifiers:
        add_borough_data_to_arr(borough.value[0], borough.value[1])
        clean_up_global_vars()

# import pandas as pd

# class RentStabFeatures:
#     boroughid = 'boroughid'
#     block = 'block'
#     lot = 'lot'

def create_bbl_column(data_frame: pd.DataFrame) -> pd.DataFrame:
    def generate_ucbbl(row):
        boroughid = str(row[RentStabFeatures.boroughid.name]).zfill(1)  # Assuming boroughid should be 1 digit
        block = str(row[RentStabFeatures.block.name]).zfill(5)  # Assuming block should be 5 digits
        lot = str(row[RentStabFeatures.lot.name]).zfill(4)  # Assuming lot should be 4 digits
        
        # Validate the inputs (example: check if they are numeric and within a certain range)
        if boroughid.isdigit() and block.isdigit() and lot.isdigit():
            return f"{boroughid}{block}{lot}"
        else:
            return None

    data_frame['ucbbl'] = data_frame.apply(generate_ucbbl, axis=1)
    return data_frame

def create_and_hidrate_db():
    summary_df = pd.read_csv('./nycdb-csvs/changes-summary.csv')
    summary_df.to_sql('rentstab', db, if_exists='replace')
    joined_df = pd.read_csv('./nycdb-csvs/joined.csv')
    joined_df.to_sql('rentstab_summary', db, if_exists='replace')
    rentstab_counts_df = pd.read_csv('./nycdb-csvs/rentstab_counts_from_doffer_2022.csv')
    rentstab_counts_df.to_sql('rentstab_v2', db, if_exists='replace')

def build_relationships():
    print('INSIDE BUILD RELATIONSHIPS')
    curs = session.connection().connection.cursor()
    print('ALL ROWS rentstabbldglistings: ')
    curs.execute('SELECT ucbbl FROM rentstabbldglistings LIMIT 5')
    # print('ALL ROWS rentstab_summary: ')
    # curs.execute('SELECT * FROM rentstab_summary LIMIT 5')
    # print('ALL ROWS rentstab: ')
    # curs.execute('SELECT * FROM rentstab LIMIT 5')
    # print('ALL ROWS rentstab_v2: ')
    # curs.execute('SELECT * FROM rentstab_v2 LIMIT 5')
    # print('results: ')
    # Join all tables by ucbbl field
    
    results = curs.fetchall()
    print(results)

    # print(curs.execute('SELECT "BBL" FROM rentstabbldglistings LIMIT 20').fetchall())

def lambda_handler(event, context):
    try :
        print(Base)
        print(db)
        print(session)
        parse_five_boroughs_pdfs()
        df = pd.DataFrame(all_data_to_insert, columns = RentStabFeatures.__members__.keys())
        df = create_bbl_column(df)
        df.to_sql(RENT_STAB_BLDG_LISTINGS, db, if_exists='replace', dtype={'ucbbl': BigInteger()})
        create_and_hidrate_db()
        # TODO: create relationships between RENT_STAB_BLDG_LISTINGS and the other tables
        build_relationships()
        
    except (Exception, psycopg2.Error) as error:
        print("Error while creating the database", error)
    finally: 
        if (session):
            session.close()
            print("Session closed")
        if (db):
            db.dispose()
            print("Database connection closed")

def connect(config):
    """Connect to the PostgreSQL database server - is the old non-sqlalchemy way of connecting to the database used only when script executed directly"""
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


if __name__ == '__main__':
    config = load_config()
    connect(config)
