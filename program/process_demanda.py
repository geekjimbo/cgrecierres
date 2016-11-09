#!/usr/bin/env python

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
import logging
import coloredlogs
from etl_controller import etl

def clean_names(filepath):
    # convert into df
    df = pd.read_csv(filepath)

    # quitar Circuitos de los nombres excepto por aquellas remotas que son ICE
    r = {'_AI\\d*': '', '_KW\\d*': '', '_\\d*Ph': '', '_kW\\d*': '', 'Unnamed: 32': 'Sin Nombre'}
    df1 = df[df['remota'].map(lambda x: '_ICE_' not in x)]
    df2 = df[df['remota'].map(lambda x: '_ICE_' in x)]
    df1['remota'] = df1['remota'].replace(r, regex=True)
    dfr = df1.append(df2)
    dfr.to_csv(filepath, index=False)
    return len(dfr)

def log_write(New_String):
    print New_String
    if not(os.path.exists(logfile_dir)):
        os.mkdir(logfile_dir)
    filename = str(datetime.now().strftime('%d%m%Y')+ '_demanda.log')

    log_time = str(datetime.now().strftime('%d%m%Y %H:%M:%S') +': ')
    with open(logfile_dir + '/' + filename , 'a+') as fh1:
        fh1.write(log_time + New_String +'\n')
        time.sleep(0.1)

def get_today_files():
    ls= subprocess.Popen(['ls', '-ltr', Data_Base_dir], stdout=subprocess.PIPE,)
    aws=subprocess.Popen(['awk','{print $9}'],stdin=ls.stdout,stdout=subprocess.PIPE,)
    end_of_pipe = aws.stdout

    arr = [] 
    narr= [] 
    for i in end_of_pipe: 
        arr.append(i)

    for i in arr:
        narr.append(i[0:len(i)-5])
    return narr

def today_files():
    narr = get_today_files()
    return narr

def old_files():
    arr = []
    df = pd.read_csv(df_dir+'demanda_df.csv')
    for i in df['file_name']:
        arr.append(i)
    return arr

def log_df():
    narr = get_today_files()
    df = pd.DataFrame(narr, columns=['file_name'])
    df.to_csv(df_dir+'demanda_df.csv', index=False)
    return len(df)

# controller
if __name__ == "__main__": 

    logname = './logs/development.log'

    logging.basicConfig(filename=logname,
        filemode='a',
        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG)

    #color logs
    coloredlogs.install()
    logging.addLevelName( logging.WARNING, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
    logging.addLevelName( logging.ERROR, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
    logging.addLevelName( logging.INFO, "\033[0;32m%s\033[1;0m" % logging.getLevelName(logging.INFO))
    logging.addLevelName( logging.DEBUG, "\033[1;36m%s\033[1;0m" % logging.getLevelName(logging.DEBUG))

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
        log_write("INFO: Copiando los archivos fuente")
        commx = 'cp -p ' + xls_dir + '*.XLS ' + Data_Base_dir
        os.system(commx)

    old_files = old_files()
    today_files = today_files()
    new_files = list(set(today_files) - set(old_files)) 

    for line in new_files:
        a=str(line.strip())
        if (a!= "" and a!="xlsx" and a != "dir"):
            print a
            temp_path = Data_Base_dir + a + '.XLS'
            print "temp path is ", temp_path
            if (os.path.exists(temp_path)):
                libO_command = 'libreoffice --headless --convert-to csv ' + temp_path + ' --outdir ' + Data_Base_dir + 'csv/'
                log_write("INFO: Converting XLS to csv: " + libO_command)
                subprocess.call(libO_command, shell=True)
                #my_local_dict["File_name"]= a
                #pickle.dump(my_local_dict , open(ctlfile_dir + 'demanda.txt' , 'wb'))
            continue

    # transform all CSV into a data_frame
    log_write("INFO: launching ETL")
    ret = etl(Data_Base_dir+"csv/")

    # substitute commas (,) for points (.) in float values
    log_write("INFO: substitute commas for points in float values")
    ret = os.popen("sed -i 's/\"\([0-9]\+\),\([0-9]\+\)\([[:space:]]*\"\)/\\1.\\2/g' "+Data_Base_dir+"csv/all_data/all_data.csv").readlines()
    ret = os.popen("sed -i 's/\"\([-]\+[0-9]\+\),\([0-9]\+\)\([[:space:]]*\"\)/\\1.\\2/g' "+Data_Base_dir+"csv/all_data/all_data.csv").readlines()

    # delete all old headers, double quotes, commas in remotas's names
    log_write("INFO: delete all old headers and  double quotes and commas in remotas names")
    ret = os.popen("sed -i 's/\(\"\)\([0-9]\+_[a-zA-Z0-9_]\+\)\(,\)\([a-zA-Z0-9]\+\)\(\"\)/\\2_\\4/g' "+Data_Base_dir+"csv/all_data/all_data.csv ").readlines()
    ret = os.popen("sed -i 's/\(\"\)\([a-zA-Z0-9]\+\)\(,\)\([a-zA-Z0-9_]\+\)\(\"\)/\\2_\\4/g' "+Data_Base_dir+"csv/all_data/all_data.csv ").readlines()

    # clean up new headers from noise
    log_write("INFO: clear new headers")
    ret = os.popen("sed -i 's/\([\d*_[a-zA-Z0-9-]*]*\)\(,\)\([a-zA-Z0-9_]*\)\(:Average\)/\\1_\\3/g' "+Data_Base_dir+"csv/all_data/all_data.csv ").readlines()
    ret = os.popen("sed -i 's/\(Belen-Filadelfia\)\(,\)/\\1_/g' "+Data_Base_dir+"csv/all_data/all_data.csv ").readlines()
    ret = os.popen("sed -i 's/\"//g' "+Data_Base_dir+"csv/all_data/all_data.csv ").readlines()

    # clean up remota's names inside df
    log_write("INFO: cleaning names like ICE")
    ret = clean_names(Data_Base_dir+"csv/all_data/all_data.csv")

    # import demandas kw data into postgres
    log_write("INFO: importing demandas kw data into postgres")
    psql_command = "psql -U postgres postgres://postgres:infografico@172.16.1.101:5432/postgres -f ~/coopecg/src/demanda/import.psql"
    ret = os.popen(psql_command).readlines()

    # move all .csv to procesados dir
    log_write("INFO: moving .csv to procesados directory")
    ret = os.popen("mv "+Data_Base_dir+"csv/*.csv "+Data_Base_dir+"csv/procesados").readlines()

    # log today's files i /df
    log_write("INFO: Log today's file in /df")
    ret = log_df()
