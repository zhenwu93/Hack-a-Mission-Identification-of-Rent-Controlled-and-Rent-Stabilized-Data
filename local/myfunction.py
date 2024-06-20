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
from psql import Base, db, session

# globals
parts = []
rows_with_extra_elements = []
all_data_to_insert = []

class BoroughIdentifiers(Enum):
    MANHATTAN = ('1', './rent-stab-pdfs/2022-DHCR-Manhattan.pdf')
    BRONX = ('2', './rent-stab-pdfs/2022-DHCR-Bronx.pdf')
    BROOKLYN = ('3', './rent-stab-pdfs/2022-DHCR-Brooklyn.pdf')
    QUEENS = ('4', './rent-stab-pdfs/2022-DHCR-Queens.pdf')
    STATEN_ISLAND = ('5', './rent-stab-pdfs/2022-DHCR-Staten-Island.pdf')

class RentStabFeatures(Enum):
    ZIP = 25.0
    BLDGNO1 = 75.0
    STREET1 = 185.0
    STSUFX1 = 312.0
    BLDGNO2 = 372.0
    STREET2 = 451.0
    STSUFX2 = 567.0
    CITY = 619.0
    COUNTY = 702.0
    STATUS1 = 769.0
    STATUS2 = 879.0
    STATUS3 = 980.0
    BLOCK = 1079.0
    LOT = 1143.0
    BOROUGH_ID = 2024.0

def visitor_body(text, cm, tm, font_dict, font_size):
    """Visitor function for extracting text from the body of the PDF"""
    y = tm[5]
    x = tm[4]
    if y > 25 and y < 865 and text and text not in RentStabFeatures.__members__:
        parts.append((text, x, y))

def create_csv(data, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

def split_data(data):
    result = []
    current_array = []
    for item in data:
        current_array.append(item)
        # issue caused by the data that does not have a lot
        if item[1] == RentStabFeatures.LOT.value:
            result.append(current_array)
            current_array = []
    return result

def fill_empty_features(data, borough_id):
    for array in data:
        for feature in RentStabFeatures:
            if (feature == RentStabFeatures.BOROUGH_ID):
                array.append((borough_id, feature.value))
            if not any(feature.value == item[1] for item in array):
                array.append(('', feature.value))
        array.sort(key=lambda x: x[1])
    return data

def remove_extra_elements(data):
    result = []
    for array in data:
        if len(array) > len(RentStabFeatures):
            rows_with_extra_elements.append(array)
        else:
            result.append(array)
    return result

def remove_tuple_data(data):
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
    result = {}
    for i, row in enumerate(data):
        key = 'fnma' + str(i)
        result[key] = row
    return result

def split_rows_with_extra_elements(data):
    result = {}
    for item in data:
        if len(item) < 3:
            continue
        elif item[2] not in result:
            result[item[2]] = []
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
        # only add data to the array if the borough is Staten Island  TEMPORARY
        if borough == BoroughIdentifiers.STATEN_ISLAND:

            add_borough_data_to_arr(borough.value[0], borough.value[1])
            clean_up_global_vars()

    #create_csv(all_data_to_insert, 'five_boroughs.csv');

def create_bbl_column(data_frame):
    BBL = 'BBL'
    data_frame2 = data_frame.copy()  # Create a copy of the DataFrame to avoid modifying the original dataset
    data_frame2[RentStabFeatures.BLOCK.name].fillna(88888888, inplace=True)
    data_frame2[RentStabFeatures.LOT.name].fillna(88888888, inplace=True)
    data_frame2[RentStabFeatures.BLOCK.name] = data_frame[RentStabFeatures.BLOCK.name].astype(int)
    data_frame2[RentStabFeatures.LOT.name] = data_frame[RentStabFeatures.LOT.name].astype(int)
    data_frame2[BBL] = data_frame[RentStabFeatures.BOROUGH_ID.name].astype(str) + data_frame[RentStabFeatures.BLOCK.name].astype(str) + data_frame[RentStabFeatures.LOT.name].astype(str)
    data_frame2[BBL] = data_frame[BBL].apply(lambda x: pd.NA if '88888888' in x else x)
    data_frame2[RentStabFeatures.BLOCK.name] = data_frame[RentStabFeatures.BLOCK.name].apply(lambda x: pd.NA if x == 88888888 else x)
    data_frame2[RentStabFeatures.LOT.name] = data_frame[RentStabFeatures.LOT.name].apply(lambda x: pd.NA if x == 88888888 else x)
    print(data_frame.head())
    return data_frame

def lambda_handler(event, context):
    try :
        print(Base)
        print(db)
        print(session)
        parse_five_boroughs_pdfs()
        df = pd.DataFrame(all_data_to_insert, columns = RentStabFeatures.__members__.keys())
        # TODO: having trouble with the bbl column.  was working in the jupyter notebook but not here
        # df = create_bbl_column(df)
        df.to_sql('test', db, if_exists='replace', index=False)
        
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
    """ Connect to the PostgreSQL database server - is the old non-sqlalchemy way of connecting to the database used only when script executed directly"""
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
