from datetime import datetime

import os
import re
import csv
import time
import logging
import pymysql
import numpy as np
import configparser
import matplotlib.pyplot as plt

LOG_FILE_NAME  = 'INIT_LEAP_BACK_{}.log'.format(datetime.now().strftime("%Y-%m-%d_%H%M%S"))
LOGS_FOLDER    = 'logs'

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


def analyze_columns(DATA_FILE_NAME):
    with open(DATA_FILE_NAME) as logFile:
        reader = csv.reader(logFile)
        head = next(reader)
        lens = []
        for an_el in head:
            lens.append([len(an_el)])
        for row in reader:
            for idx, a_str in enumerate(row):
                if len(a_str) > 90: print(a_str)
                lens[idx].append(len(a_str) )
    
    prows = len(head)//2
    f, ax = plt.subplots(prows,prows)
    for index, dist in enumerate(lens):
        print("[{}] Fixed:{}, mean:{} median:{} max:{}".format(head[index],len(set(dist)) == 1,np.mean(dist), np.median(dist), max(dist)))
        xp = (index)%prows; yp = (index)//prows; 
        ax[xp,yp].hist(dist)
        ax[xp, yp].set_title(head[index])
    plt.show()

def push_to_db(DATA_FILE_NAME):
    with open(DATA_FILE_NAME) as logFile:
        pass


# ES Log specific ##################################################################################################################################################
ES_DATA_FILE_NAME = "ES_EVENT_LOG_v11.csv"
def es_push_to_db():
    with open(ES_DATA_FILE_NAME) as logFile:
        reader = csv.reader(logFile)
        head = next(reader)
        for row in reader:
            dml = """
            INSERT INTO LEAP.ES_EVENT_LOG
            VALUES(
                "{}",  {} , "{}",
                "{}", "{}", "{}",
                STR_TO_DATE('{} {}','%d.%m.%Y %H:%i:%s'),
                STR_TO_DATE('{} {}','%d.%m.%Y %H:%i:%s'),
                            "{}",
                "{}", "{}", "{}",
                "{}", "{}", "{}",
                "{}", "{}", "{}"
            )
            """.format(
                row[0], row[1], row[2],
                row[3], row[4], row[5],
                row[6],  row[7],
                row[ 9], row[10], 
                                  row[11], 
                row[12], row[13], row[14], 
                row[15], row[16], row[17], 
                row[18], row[19], row[20]
            )
            DB_cursor.execute(dml)
        DB_cursor.execute('commit')
#####################################################################################################################################################################

# 50M test Log specific #############################################################################################################################################
M50_DATA_FILE_NAME = "data/50M_TEST.csv"
def m50_push_to_db():
    with open(M50_DATA_FILE_NAME) as logFile:
        reader = csv.reader(logFile)
        head = next(reader)
        for row in reader:
            rowNew = list(row)
            rowNew[6] = rowNew[6] if rowNew[6].strip() else 'NULL'
            dml = """
            INSERT INTO LEAP.TEST_50M
            VALUES {}
            """.format(tuple(rowNew)).replace('\'NULL\'', 'NULL')
            DB_cursor.execute(dml)
        DB_cursor.execute('commit')

M50H_DATA_FILE_NAME = "data/50M_TEST_header.csv"
def m50h_push_to_db():
    with open(M50H_DATA_FILE_NAME) as logFile:
        reader = csv.reader(logFile)
        head = next(reader)
        for row in reader:
            rowNew = list(row)
            dml = """
            INSERT INTO LEAP.TEST_50M_HEADER
            VALUES {}
            """.format(tuple(rowNew)).replace('\'NULL\'', 'NULL')
            DB_cursor.execute(dml)
        DB_cursor.execute('commit')
#####################################################################################################################################################################

# Run ---------------------------------------------------------------------------------------------------------------------------
m50h_push_to_db()
# Hosekeeping -------------------------------------------------------------------------------------------------------------------
DB_cursor.close(); connection.close()
