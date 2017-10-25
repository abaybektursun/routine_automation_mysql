from datetime import datetime

import os
import re
import csv
import time
import json
import glob
import string
import logging
import pymysql
import numpy as np
import configparser
import dateutil.parser
import matplotlib.pyplot as plt

#----------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------------------
LOG_FILE_NAME  = 'MySQL_auto_{}.log'.format(datetime.now().strftime("%Y-%m-%d_%H%M%S"))
LOGS_FOLDER    = 'logs'
COLUMN_TYPE    = {}
COLUMN_NAMES   = []
DB             = 'LEAP'
# Set up logs
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)

logging.basicConfig(format='%(levelname)s\t%(asctime)s\t%(message)s', filename='{}/{}'.format(LOGS_FOLDER,LOG_FILE_NAME), datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
logging.info('Application has started')

dbconfigs = configparser.ConfigParser()
dbconfigs.read('msql.dbcredentials')
try: connection   = pymysql.connect(
    host     = dbconfigs['default']['HOST'],
    user     = dbconfigs['default']['USER'],
    password = dbconfigs['default']['PASS'],
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
);
except Exception as ex: logging.error(str(ex)); exit()
DB_cursor = connection.cursor()
#----------------------------------------------------------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------------------------
def decide_dtype(field):
    total = sum(field.values())
    field_ratios = {k: v/total for k, v in field.items()}
    if 'NULL'        in field_ratios and field_ratios['NULL']     >= 0.9: return None
    if 'DATETIME'        in field_ratios and field_ratios['DATETIME'] == 1.0: return 'DATETIME'
    if  len(field_ratios) == 2 and \
            'DATETIME' in field_ratios and \
            'NULL'     in field_ratios: return 'DATETIME'
    if  len(field_ratios) > 1  and \
            'DATETIME' in field_ratios and \
            'DOUBLE'  in field_ratios: return 'TEXT'
    if  len(field_ratios) > 1  and \
            'DATETIME' in field_ratios and \
            'INT'      in field_ratios: return 'TEXT'
    if 'TEXT'          in field_ratios: return 'TEXT'
    if 'VARCHAR(50)'   in field_ratios: return 'VARCHAR(50)'
    if 'DOUBLE'       in field_ratios: return 'DOUBLE'
    if 'BIGINT'        in field_ratios: return 'BIGINT'
    return 'INT'
    #print(json.dumps(field_ratios,indent=4)) 

def parse_type(in_str):
    string = in_str.strip()
    if len(string) > 50: return 'TEXT'
    if not string or string.lower() == 'null': return 'NULL'
    try: 
        if int(string) < (2**32)//2:
            return 'INT'
        else: return 'BIGINT'
    except: pass
    try: float(string); return 'DOUBLE'; 
    except: pass
    try: dateutil.parser.parse(string); return 'DATETIME'
    except: pass
    return 'VARCHAR(50)'

#----------------------------------------------------------------------------------------------------------------------------------------------
def analyze_columns(DATA_FILE_NAME):
    global COLUMN_NAMES
    with open(DATA_FILE_NAME) as logFile:
        reader = csv.reader(logFile)
        COLUMN_NAMES   = next(reader)
        COLUMN_NAMES   = [re.sub('[^0-9a-zA-Z]+', '_', a_name.strip()) for a_name in COLUMN_NAMES]
        print(COLUMN_NAMES)
        for cname in COLUMN_NAMES:
            duplic_indices = [i for i, x in enumerate(COLUMN_NAMES) if x == cname]
            if len(duplic_indices) > 1:
                for i in duplic_indices: COLUMN_NAMES[i] = COLUMN_NAMES[i] + str(i)
        lens   = []
        for an_el in COLUMN_NAMES:
            lens.append([len(an_el)])
        
        types = {}; 
        for fname in COLUMN_NAMES: types[fname] = {}

        for row_num, row in enumerate(reader):
            if row_num%3000 == 0: print('Row Number: '+str(row_num))
            for idx, a_str in enumerate(row):
                fname = COLUMN_NAMES[idx]; dtype = parse_type(a_str)
                if dtype in types[fname]: types[fname][dtype] += 1
                else: types[fname][dtype] = 1
                lens[idx].append(len(a_str))

        print(json.dumps(types, indent=4))
        for f in types: COLUMN_TYPE[f] = decide_dtype(types[f])
    
    '''prows = len(COLUMN_NAMES)//2
    if len(COLUMN_NAMES) < 10: f, ax = plt.subplots(prows,prows)

    for index, dist in enumerate(lens):
        print("[{}] Fixed:{}, mean:{} median:{} max:{}".format(COLUMN_NAMES[index],len(set(dist)) == 1,np.mean(dist), np.median(dist), max(dist)))

        if len(COLUMN_NAMES) < 10:
            xp = (index)%prows; yp = (index)//prows; 
            ax[xp,yp].hist(dist)
            ax[xp, yp].set_title(COLUMN_NAMES[index])
    if len(COLUMN_NAMES) < 10: plt.show()'''

#----------------------------------------------------------------------------------------------------------------------------------------------
def push_to_db(DATA_FILE_NAME):
    tbl_name = DATA_FILE_NAME.split('/')[-1].strip()
    if not tbl_name: tbl_name = filenm.split('/')[-2]
    tbl_name = tbl_name.split('.')[0]
    tbl_name = "{}.`{}`".format(DB,tbl_name)
    with open(DATA_FILE_NAME) as logFile:
        reader = csv.reader(logFile)
        next(reader)
        dml_base = "INSERT INTO "+tbl_name +" ({cnames}) VALUES({vals});"
        
        for row_num, row in enumerate(reader):
            names = ""; vals = ""
            for idx, a_str in enumerate(row):
                ctype = COLUMN_TYPE[COLUMN_NAMES[idx]]
                if ctype:
                    names += COLUMN_NAMES[idx]+','
                    if not a_str.strip() or a_str.strip().upper() == 'NULL': vals += 'NULL,'
                    elif ctype in ['TEXT','VARCHAR(50)']:
                        vals  += "'{}',".format(a_str.replace('\n',' ').replace("'","\\'").replace('"',"\\'").encode('ascii','ignore').decode())
                    elif ctype in ['DATETIME']:
                        vals  += "'{}',".format(dateutil.parser.parse(a_str).strftime('%Y-%m-%d %H:%M:%S') ) 
                    else: vals += a_str + ','
            if row_num%3000 == 0: 
                print('Pushing Row Number: '+str(row_num))
                print(dml_base.format(cnames=names,vals=vals))
            names = names[:-1]
            vals  = vals[:-1]
            try:
                DB_cursor.execute(dml_base.format(cnames=names,vals=vals))
            except : print(dml_base.format(cnames=names,vals=vals)); exit()

#----------------------------------------------------------------------------------------------------------------------------------------------

def generate_ddl(filenm):
    tbl_name = filenm.split('/')[-1].strip()
    if not tbl_name: tbl_name = filenm.split('/')[-2]
    tbl_name = tbl_name.split('.')[0]
    tbl_name = "{}.`{}`".format(DB,tbl_name)

    ddl_base = "CREATE TABLE {tblName}({columns})"
    columns  = ""
    for cname in COLUMN_TYPE:
        if COLUMN_TYPE[cname]: columns += "{} {},".format(cname, COLUMN_TYPE[cname])
    columns = columns[:-1]
    ddl_base = ddl_base.format(tblName=tbl_name,columns=columns)

    return ddl_base

# Run ---------------------------------------------------------------------------------------------------------------------------
def context_run(dFile):
    global COLUMN_TYPE; global COLUMN_TYPE;
    COLUMN_TYPE  = {}
    COLUMN_NAMES = []
    analyze_columns(dFile)
    DB_cursor.execute(generate_ddl(dFile))
    print(generate_ddl(dFile))
    push_to_db(dFile)

glob_file_search_string = '/home/abaybektursun/Desktop/LEAP_LOG_DATA/**/*.csv'
for dFile in glob.iglob(glob_file_search_string, recursive=True):
    print('-'*150)
    print('Starting: ' + dFile)
    try:
        context_run(dFile)
        DB_cursor.execute('commit;')
    except Exception as e:
        logging.error("{}:{}".format(dFile,str(e)))
    print('-'*150)


# Hosekeeping -------------------------------------------------------------------------------------------------------------------
DB_cursor.close(); connection.close()


