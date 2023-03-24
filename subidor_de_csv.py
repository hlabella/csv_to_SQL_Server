# -*- coding: utf-8 -*-
"""
subidor de csv
@author: HLABELLA
@purpose: subir uma tabela com f9 enter
"""

import pandas as pd
import os.path
import os
import glob
import pyodbc
from pathlib import Path

path_to_download_folder = str(os.path.join(Path.home(), "Downloads"))

#here you have to change your downloads folder path:
filename = max(glob.iglob(path_to_download_folder+"\*.csv"), key=os.path.getmtime)


checa = input("check the name:\n"+filename+"\nY/N?\n")

if (checa == 'Y' or checa == '') and '.csv' in filename:

    print("Uploading...\n")
    #puxa como df
    df_final = pd.read_csv(filename, sep=';' , encoding='latin-1')
    
    #here you have to change to your ODBC
    pd.set_option('display.float_format', lambda x: '%.3f' % x)
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=10.128.222.10;'
                          'Database=CDG;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()

    #Get rid of invalid characters in the column names
    Col_list = []
    for i in range(df_final.shape[1]):   
        Col_Name = df_final.columns[i].replace(" - ", "_").replace(" ", "_").replace("&", "and") # Some characters were unacceptable
        Col_list.append(Col_Name)
    df_final.columns = Col_list
    
    
    #This function creates a create table statement out of my dataframe columns and data types
    Text_list = []
    for i in range(df_final.shape[1]):   
        Col_Name = df_final.columns[i]
        Python = df_final.convert_dtypes().dtypes[i]  # Most of the data types were listed as object so this reassigns them
        if Python == float:
            Oracle = "FLOAT"
        elif Python == 'datetime64[ns]':
            Oracle = 'DATE'
        elif Python == 'Int64':
            Oracle = "BIGINT"
        else:
            Oracle = "VARCHAR(1000)"
        Text_list.append(Col_Name)
        Text_list.append(' ')
        Text_list.append(Oracle)
        if i < (df_final.shape[1] - 1): 
            Text_list.append(", ")
    Text_Block = ''.join(Text_list)
    
    cursor = conn.cursor()
    
    #EXTRAI O NOME E CRIA A TABELA
    
    batizado = os.path.basename(filename)
    
    if '.csv' in batizado:
        
        batizado = batizado[:-4]
        
        create = "CREATE TABLE CDG.." + batizado + " (   {}    )".format(Text_Block)
        cursor.execute(create)
        cursor.execute("commit")
        cursor.close()

        # insert
        interrogs = ''
        for x in range(len(df_final.columns)):
            interrogs += '?,'
        interrogs = interrogs[:-1]
        insert_to_tmp_tbl_stmt = f"INSERT INTO CDG.."+batizado+" VALUES ("+interrogs+")"
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(insert_to_tmp_tbl_stmt, df_final.values.tolist())
        cursor.commit()
        cursor.close()
        print("finalizado!\n")
        
    else:
        print("error: not .csv\n")
else:
    print("error: denied or not .csv\n")   