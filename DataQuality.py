# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 09:20:32 2019

@author: dariy
"""

from tqdm import tqdm 
from multiprocessing import Pool
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
logFileName = f"_log_RawDataQuality"
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
def ConsistentFeetAltitudeBin( row_id, update=True ):
    
    try:
        q = f"select top 1 * from RawData where [ID] = {row_id}"
        rows = SQLExec(q)
        if len(rows):
            r = rows[0]
        else:
            return None
        
        if r.Altitude_bin is None:
            return None  
        
        qid = None
        q = f"select top 1 QID from RawDataQuality where RawData_ID = {r.ID}"
        rows = SQLExec( q )
        if len(rows):
            qid = rows[0].QID
        
        rbin = r.Altitude_bin.split(' ')
        if rbin[0]=='<' and r.Feet_above_ground <= int(rbin[1])\
            or rbin[0]=='>' and r.Feet_above_ground > int(rbin[1]):
                Consistent_feet_altitude_bin = 1
        else:
            Consistent_feet_altitude_bin = 0

        if qid is None:
            columns = "[RawData_ID], [Consistent_feet_altitude_bin]"
            values = f"{r.ID}, {Consistent_feet_altitude_bin}"
            q = f"INSERT INTO RawDataQuality ({columns}) VALUES ({values})"
            _ = SQLExec(q, 1)
            
        elif update:
            q = f'''UPDATE RawDataQuality 
                    SET Consistent_feet_altitude_bin={Consistent_feet_altitude_bin}
                    WHERE QID={qid}'''
            _ = SQLExec(q, 1)
            
    except:
        log.exception( f'RowData_ID: {row_id}' )
        
    return 
    
#%%
def DoRecord(row_id):
    
    _ = ConsistentFeetAltitudeBin( row_id )
    
    return None
#%%
if __name__ == "__main__":
    
    q = "select min(ID) id_min, max(ID) id_max from RawData"
    id_min, id_max = SQLExec( q )[0]
    
    # Multi-processing records
    with Pool( processes=40 ) as pool:
        for _ in tqdm( pool.imap_unordered(DoRecord, range(id_min,id_max+1)), 
                      total=id_max, desc="Processing Records for Data Quality" ):
            pass