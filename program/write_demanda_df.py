#!/usr/bin/python

import os
import json
import subprocess
import time
import pdb
import psycopg2
import sys
import pickle
import os
import pandas as pd
from datetime import datetime
from openpyxl import load_workbook

def log_write(New_String):
    print New_String
    if not(os.path.exists(logfile_dir)):
        os.mkdir(logfile_dir)
    filename = str(datetime.now().strftime('%d%m%Y')+ '_demanda.log')

    log_time = str(datetime.now().strftime('%d%m%Y %H:%M:%S') +': ')
    with open(logfile_dir + '/' + filename , 'a+') as fh1:
        fh1.write(log_time + New_String +'\n')
        time.sleep(0.1)

with open('/home/infografico/coopecg/src/demanda/configuracion.json', 'r') as f:
    data_json = json.load(f)
    Data_Base_dir = data_json["Variable_Database"]["source_path"]
    postgresql_path = data_json["Variable_Database"]["postgresql_connect"]
    logfile_dir=data_json["Variable_Database"]["logfile_path"]
    ctlfile_dir=data_json["Variable_Database"]["control_path"]
    xls_dir = data_json["Variable_Database"]["xls_path"]
    df_dir = data_json["Variable_Database"]["df_path"]

log_write("Genera dataframe de XLS en el path indicado")
if not(os.path.exists(df_dir)):
    log_write("ERROR: No se puede accesar la carpeta fuente de DF")
    sys.exit(1)
else:
    log_write("INFO: Generando DF en el path indicado")

ls= subprocess.Popen(['ls', '-ltr', Data_Base_dir], stdout=subprocess.PIPE,)
aws=subprocess.Popen(['awk','{print $9}'],stdin=ls.stdout,stdout=subprocess.PIPE,)
end_of_pipe = aws.stdout

arr = [] 
narr= [] 
for i in end_of_pipe: 
    arr.append(i)

for i in arr:
    narr.append(i[0:len(i)-5])

df = pd.DataFrame(narr, columns={'file_name'})
df.to_csv(df_dir+'demanda_df.csv', index=False, headers='true')
