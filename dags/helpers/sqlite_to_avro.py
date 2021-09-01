import os,sys
import pickle
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import json
import datetime

import pandas as pd 
import numpy as np 
import sqlite3
import pyarrow as pa
import pyarrow.parquet as pq
import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from airflow.models.variable import Variable


def load_avro_schema(schema_filepath):
    schema = avro.schema.Parse(
        json.dumps(
            schema_filepath
        )
    )
    return schema

def convert_val(val,mode):
    if val:
        if mode=='int':
            return int(val)
        elif mode=='float':
            return float(val)
        elif mode=='time-millis':
            return val
        elif mode == 'date':
            format_ = "%Y-%m-%d %H:%M:%S"
            ts_obj = datetime.datetime.strptime(val,format_)
            date_int = 10000*ts_obj.year + 100*ts_obj.month + ts_obj.day
            return date_int
        else:
            return val
    else:
        return val


def sqlite_to_avro(sqlite_db,query_path,avro_schema_path,destination_path):
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    query_file = open(query_path,'r')
    query = query_file.read()
    query_file.close()

    con = sqlite3.connect(sqlite_db)
    con.row_factory = dict_factory
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    #close connection to sqlite database
    cur.close()
    con.close()

    file = open(destination_path, 'wb')
    datum_writer = DatumWriter()
    

    with open(avro_schema_path,'rb') as f:
        schema_dict = json.load(f)
    
    schema = avro.schema.parse(
        open(avro_schema_path, "rb").read()
    )

    row_writer = DataFileWriter(file, datum_writer, schema)

    fields = schema_dict['fields']

    for dat in rows:
        
        # datum = {}
        # for field in fields:
        #     type_ = field['type'][0]
        #     mode = type_
        #     logic = None
        #     if 'logicalType' in field:
        #         logic = field['logicalType']
        #         if mode == 'int' and logic=='time-millis':
        #             mode = 'time-millis'
        #         elif mode == 'int' and logic=='date':
        #             mode  = 'date'

        #     datum[field['name']] = convert_val(dat.get(field['name']), mode)

        row_writer.append(
            dat
        )
        # print(dat)

    
    row_writer.close()


    return True


# sqlite_to_avro(
#     "/Users/kurniawankesumaputra/Desktop/programming/python/airflow_home/data/database.sqlite",
#     "/Users/kurniawankesumaputra/Desktop/programming/python/airflow_home/data/sqlite_data/player_attributes.sql",
#     '/Users/kurniawankesumaputra/Desktop/programming/python/airflow_home/dags/schema_file/league.json',
#     "/Users/kurniawankesumaputra/Desktop/programming/python/airflow_home/data_result/league_avro.avro"
# )