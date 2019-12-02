# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 09:51:55 2019

@author: dariy
"""

import pandas as pd, re
from tqdm import tqdm 
import pyodbc

#%% Logging            
import logging, io, traceback
class MyFormatter(logging.Formatter):
    def __init__(self, fmt):
        logging.Formatter.__init__(self, fmt)
    def formatException(self, ei):
        sio = io.StringIO()
        traceback.print_exception(ei[0], ei[1], ei[2], None, sio, chain=False)
        s = sio.getvalue()
        sio.close()
        return s
logFileName = f"_log_BirdStrike"
log = logging.getLogger( logFileName )
log.propagate = False
log.setLevel( logging.DEBUG )
fh = logging.FileHandler(logFileName, encoding = "UTF-8-sig")
formatter = MyFormatter('%(levelname)s _,_ %(asctime)s _,_ %(message)s')
fh.setFormatter(formatter)
log.addHandler(fh)
    
#%%
conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};'\
                +'SERVER=birdstrike.cdstk7wwp9vq.us-east-1.rds.amazonaws.com;'\
                +'DATABASE=birdstrike;UID=admin;PWD=admin8351'

# From Database; RawData column - data_type dict
with pyodbc.connect(conn_string) as conn:
    with conn.cursor() as cursor:
#        print(list(cursor.columns(table='RawData')))
        raw_data_columns = {r.column_name: r.type_name 
                            for r in cursor.columns(table='RawData')}
def SQLExec(q , commit=False):
    with pyodbc.connect(conn_string, autocommit=commit) as conn:
        with conn.cursor() as cursor:
            cursor.execute(q)
            if commit: 
                rows = []
            else:
                rows = cursor.fetchall()
                
    return rows

#%%
from datetime import date, timedelta
def Decorator(func, x): # Decorator(bool,2) 
    if func == date:
        func = str
    return func(x)
#%%
def DoRecord( record ):
    
    try:
        if record['Record_ID']=='':
            return None
        
        q = f"select top 1 * from RawData where [Record_ID] = { record['Record_ID'] }"
        row = SQLExec( q )
    
        if len(row) == 0: # insert record into RawData
            columns, values = [], []
            for k, v in record.items():
                if v=='':
                    continue
                columns.append( k )
                if type(v) == type('string'):
                    v = v.replace('\'','\'\'')
                    
                if raw_data_columns[ k ].lower() in ['nvarchar', 'bit', 'date']:
                    values.append( f"N'{v}'" )
                else:
                    values.append( v )
            columns = ', '.join( columns )
            values = ', '.join( [str(v) for v in values] )
            
            q = f"INSERT INTO RawData ({columns}) VALUES ({values})"
            _ = SQLExec( q , True)
    
        else: # update row
            # apply DB data types to record and compare row and record items
            row = row[0]
            non_shared = {}
            for i, f in enumerate( row.cursor_description ):
                # ignore ID, Insertion_date and Nulls
                if f[0] in ['ID','Insertion_date']\
                or f[0] not in record\
                or (record[f[0]]=='' and row[i] is None): 
                    continue
                
                if row[i] is not None and  record[f[0]] == '':
                    non_shared[f[0]] = ''
                    
                elif record[f[0]] != '':
                    record[f[0]] = Decorator(f[1], record[f[0]])           
                    if row[i] is None or record[f[0]] != Decorator(f[1], row[i]):
                        non_shared[f[0]] = record[f[0]]
            
            updates = [] 
            for k, v in non_shared.items():
                if type(v) == type('string'):
                    v = v.replace('\'','\'\'')
                    
                if raw_data_columns[ k ].lower() in ['nvarchar', 'bit', 'date']:
                    updates.append( f"{k}=N'{v}'" )
                else:
                    updates.append( f"{k}={v}" )
    
            if len(updates):
                updates = ', '.join( updates )
                q = f"UPDATE RawData SET {updates} where ID = {row.ID}"
                SQLExec(q, True)
    except:
        log.exception( record )
    
    return None

#%%
def ApplyBussinessRules( record ):
#%%
def PrepareRecordsAndDBTable( path ):       
    # loading and preprocessing data frame
    df = pd.read_csv(path, encoding='ISO-8859-1')
    df['FlightDate'] = pd.to_datetime(df['FlightDate'], dayfirst=True)
    df['FlightDate'] = df['FlightDate'].astype('str').replace('NaT','')
    df['Feet above ground'] = df['Feet above ground'].str.replace(',','')
    df['Cost: Total $'] = df['Cost: Total $'].str.replace(',','')
    df = df.fillna('')
    
    # Adding new columns into RawData       
    df_column_normalized = []
    for column in tqdm( df.columns, desc="Preparing records and RawData table" ):
        
        # Normalize column names
        column_normalized = re.sub('\W+','_',column).strip('_')
        if len(column_normalized)==0 or column_normalized[0].isnumeric():
            column_normalized = '_' + column_normalized
        df_column_normalized.append( column_normalized )
            
        #
        if column_normalized in raw_data_columns:
            continue
    
        data_type = df[column].dtype.name
        if data_type.startswith('int'):
            data_type = 'int'
        elif data_type.startswith('float'):
            data_type = 'float'
        elif data_type.startswith('bool'):
            data_type = 'bit'
        else:
            data_type = 'nvarchar (255)'
            
        q = f"ALTER TABLE RawData ADD {column_normalized} {data_type};"
        _ = SQLExec(q, True)

    df.columns = df_column_normalized
    
    # If it is the final file, delete from RawData records those are not in final file any more
    if path.lower().endswith('final.csv'):
        record_ids = set( df['Record_ID'] )
        base_date = str( date.today() - timedelta(days=4) )
        q = f"select ID, Record_ID from RawData where Insertion_date >= '{ base_date }'"
        rows = SQLExec( q )
        for r in tqdm(rows, desc='Deleting records that are not in final file'):
            if r.Record_ID not in record_ids:
                q = f"delete from RawData where ID = {r.ID}"
                _ = SQLExec(q, True)
                
    return df                 
            
#%%
if __name__ == "__main__":
    
    # Loading and Preparing records
#    path = f'./data/Bird Strikes - Base.csv'
#    path = f'./data/Bird Strikes - Day 1.csv'
#    path = f'./data/Bird Strikes - Day 2.csv'
#    path = f'./data/Bird Strikes - Day 3.csv'
    path = f'./data/Bird Strikes - Final.csv'
    
    df = PrepareRecordsAndDBTable( path )
    records = df.to_dict('records')
    # record = records[315]
    
    # Multi-processing records
    from multiprocessing import Pool
    with Pool( processes=40 ) as pool:
        for _ in tqdm( pool.imap_unordered(DoRecord, records), 
                        total=len(records), desc="Processing Records" ):
            pass
            
        
        
            
            

    