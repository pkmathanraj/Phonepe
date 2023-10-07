import git
import json
import glob
import pandas as pd
import pathlib
from pathlib import Path
from mysql.connector import connect, Error
from configparser import ConfigParser
from os import path

# Cloning the PhonePe Pulse Dataset from the github
def clone():
    # Checking the directory already exists before cloning
    # Update this with by checking the last commit from the github - thereby it is dynamically updatable
    if path.isdir("pulse"):
        pass
    else:
        git.Git("data").clone("https://github.com/PhonePe/pulse")

# Extracting all the json file names with path from the downloaded / cloned dataset
def read_dir():
    filenames=[name for name in glob.glob('pulse\data\**\*.json', recursive=True)]
    #print(len(filenames))
    return filenames

# Extracting Database Configuration from file
def config(filename='database.ini', section='mysql'):
    parser = ConfigParser()
    parser.read(filename)
  
    # get section, default to mysql
    conn_param = {}
    if parser.has_section(section):
        params = parser.items(section)
        for p in params:
            conn_param[p[0]] = p[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
  
    return conn_param

# Establishing MySQL Connection
def db_connection():
    params = config()
    connection = None
    try:
        connection = connect(**params)
    except Error as e:
        print("Error during establishing MySQL connection: ",e)
    return(connection)

# Creating tables in MySQL
def create_mysqlschema():
    conn = db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE IF NOT EXISTS phonepe")
            cursor.execute("USE phonepe")
    except Error as e:
        print("Error during Db creation: ",e)
    
    query1 = "CREATE TABLE IF NOT EXISTS agg_trans_country(\
        country VARCHAR(20),\
        year YEAR,\
        quarter TINYINT,\
        transaction_type VARCHAR(40),\
        transaction_count BIGINT,\
        transaction_amount DOUBLE)"
    query2 = "CREATE TABLE IF NOT EXISTS agg_trans_state(\
        country VARCHAR(20),\
        state VARCHAR(40),\
        year YEAR,\
        quarter TINYINT,\
        transaction_type VARCHAR(40),\
        transaction_count BIGINT,\
        transaction_amount DOUBLE)"
    query3 = "CREATE TABLE IF NOT EXISTS agg_user_country(\
        country VARCHAR(20),\
        year YEAR,\
        quarter TINYINT,\
        brand_name VARCHAR(40),\
        user_count BIGINT,\
        percentage DOUBLE)"
    query4 = "CREATE TABLE IF NOT EXISTS agg_user_state(\
        country VARCHAR(20),\
        state VARCHAR(40),\
        year YEAR,\
        quarter TINYINT,\
        brand_name VARCHAR(40),\
        user_count BIGINT,\
        percentage DOUBLE)"
    query5 = "CREATE TABLE IF NOT EXISTS map_trans_country(\
        country VARCHAR(20),\
        year YEAR,\
        quarter TINYINT,\
        state VARCHAR(40),\
        transaction_count BIGINT,\
        transaction_amount DOUBLE)"
    query6 = "CREATE TABLE IF NOT EXISTS map_trans_state(\
        country VARCHAR(20),\
        state VARCHAR(40),\
        year YEAR,\
        quarter TINYINT,\
        distrct VARCHAR(40),\
        transaction_count BIGINT,\
        transaction_amount DOUBLE)"
    query7 = "CREATE TABLE IF NOT EXISTS map_user_country(\
        country VARCHAR(20),\
        year YEAR,\
        quarter TINYINT,\
        state VARCHAR(40),\
        registered_users BIGINT,\
        app_opens BIGINT)"
    query8 = "CREATE TABLE IF NOT EXISTS map_user_state(\
        country VARCHAR(20),\
        state VARCHAR(40),\
        year YEAR,\
        quarter TINYINT,\
        district VARCHAR(40),\
        registered_users BIGINT,\
        app_opens BIGINT)"
    query9 = "CREATE TABLE IF NOT EXISTS top_trans_country(\
        country VARCHAR(20),\
        year YEAR,\
        quarter TINYINT,\
        cat_type VARCHAR(15),\
        type_name VARCHAR(40),\
        transaction_count BIGINT,\
        transaction_amount DOUBLE)"
    query10 = "CREATE TABLE IF NOT EXISTS top_trans_state(\
        country VARCHAR(20),\
        state VARCHAR(40),\
        year YEAR,\
        quarter TINYINT,\
        cat_type VARCHAR(15),\
        type_name VARCHAR(40),\
        transaction_count BIGINT,\
        transaction_amount DOUBLE)"
    query11 = "CREATE TABLE IF NOT EXISTS top_user_country(\
        country VARCHAR(20),\
        year YEAR,\
        quarter TINYINT,\
        cat_type VARCHAR(15),\
        type_name VARCHAR(40),\
        registered_users BIGINT)"
    query12 = "CREATE TABLE IF NOT EXISTS top_user_state(\
        country VARCHAR(20),\
        state VARCHAR(40),\
        year YEAR,\
        quarter TINYINT,\
        cat_type VARCHAR(15),\
        type_name VARCHAR(40),\
        registered_users BIGINT)"

    try:
        with conn.cursor() as cursor:
            query_lst=[query1, query2, query3, query4, query5, query6, query7, query8, query9, query10, query11, query12]
            operation = ";".join(query_lst)
            cursor.execute(operation, multi=True)
                
    except Error as e:
        print("Error during table creation: ",e)
    
    conn.close()
        
# Extracting data from JSON file and Storing in MySQL
def extract_data(filepaths):
    conn = db_connection()
    rows_check = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("USE phonepe")
            cursor.execute("SELECT * from agg_trans_country")
            rows_check = cursor.fetchall()
            
    except Error as e:
        print("Error : ",e)
    conn.close()

    # Data Extraction from json and Storing to MySQL after checking that there is no data in the database table
    # It's necessary to check to avoid inserting duplicate records
    if not rows_check:
        aggr_trans, aggr_user = [], []
        map_trans, map_user = [], []
        top_trans, top_user = [], []
        agg_trans_country, agg_trans_state = [], []
        agg_user_country, agg_user_state = [], []
        map_trans_country, map_trans_state = [], []
        map_user_country, map_user_state = [], []
        top_trans_country, top_trans_state = [], []
        top_user_country, top_user_state = [], []
        state = ""
        for file in filepaths:
            path = pathlib.PurePath(file)
            #print("parent: ",path.parent)
            #print("parent name: ",path.parent.name)        
            #print(file)
            if Path(rf"{path}").parts[2]=='aggregated' and Path(rf"{path}").parts[3]=='transaction':
                aggr_trans.append(file) 
                jsonfile = json.load(open(file))    
                country, year, quarter = Path(rf"{path}").parts[5], Path(rf"{path}").parts[-2], (Path(rf"{path}").parts[-1]).strip('.json')
                #state = ""
                if Path(rf"{path}").parts[6]=='state':
                    state = Path(rf"{path}").parts[7]
                for i in jsonfile['data']['transactionData']:
                    if Path(rf"{path}").parts[6]=='state':
                        agg_trans_state.append((country, state, year, quarter, i['name'], i['paymentInstruments'][0]['count'], i['paymentInstruments'][0]['amount']))    
                    else:
                        agg_trans_country.append((country, year, quarter, i['name'], i['paymentInstruments'][0]['count'], i['paymentInstruments'][0]['amount']))

            elif Path(rf"{path}").parts[2]=='aggregated' and Path(rf"{path}").parts[3]=='user':
                aggr_user.append(file)
                jsonfile = json.load(open(file))    
                country, year, quarter = Path(rf"{path}").parts[5], Path(rf"{path}").parts[-2], (Path(rf"{path}").parts[-1]).strip('.json')
                #state = ""
                if Path(rf"{path}").parts[6]=='state':
                    state = Path(rf"{path}").parts[7]
                if jsonfile['data']['usersByDevice']: # Checking for Not Null
                    for i in jsonfile['data']['usersByDevice']:
                        if Path(rf"{path}").parts[6]=='state':
                            agg_user_state.append((country, state, year, quarter, i['brand'], i['count'], i['percentage']))    
                        else:
                            agg_user_country.append((country, year, quarter, i['brand'], i['count'], i['percentage']))

            elif Path(rf"{path}").parts[2]=='map' and Path(rf"{path}").parts[3]=='transaction':
                map_trans.append(file)     
                jsonfile = json.load(open(file))    
                country, year, quarter = Path(rf"{path}").parts[6], Path(rf"{path}").parts[-2], (Path(rf"{path}").parts[-1]).strip('.json')
                #state = ""
                if Path(rf"{path}").parts[7]=='state':
                    state = Path(rf"{path}").parts[8]
                for i in jsonfile['data']['hoverDataList']:
                    if Path(rf"{path}").parts[7]=='state':
                        map_trans_state.append((country, state, year, quarter, i['name'], i['metric'][0]['count'], i['metric'][0]['amount']))    
                    else:
                        map_trans_country.append((country, year, quarter, i['name'], i['metric'][0]['count'], i['metric'][0]['amount']))

            elif Path(rf"{path}").parts[2]=='map' and Path(rf"{path}").parts[3]=='user':
                map_user.append(file)
                jsonfile = json.load(open(file))    
                country, year, quarter = Path(rf"{path}").parts[6], Path(rf"{path}").parts[-2], (Path(rf"{path}").parts[-1]).strip('.json')
                #state = ""
                if Path(rf"{path}").parts[7]=='state':
                    state = Path(rf"{path}").parts[8]
                for i in jsonfile['data']['hoverData']:
                    if Path(rf"{path}").parts[7]=='state':
                        map_user_state.append((country, state, year, quarter, i, jsonfile['data']['hoverData'][i]['registeredUsers'], jsonfile['data']['hoverData'][i]['appOpens']))    
                    else:
                        map_user_country.append((country, year, quarter, i, jsonfile['data']['hoverData'][i]['registeredUsers'], jsonfile['data']['hoverData'][i]['appOpens']))

            elif Path(rf"{path}").parts[2]=='top' and Path(rf"{path}").parts[3]=='transaction':
                top_trans.append(file)     
                jsonfile = json.load(open(file))    
                country, year, quarter = Path(rf"{path}").parts[5], Path(rf"{path}").parts[-2], (Path(rf"{path}").parts[-1]).strip('.json')
                #state = ""
                if Path(rf"{path}").parts[6]=='state':
                    state = Path(rf"{path}").parts[7]
                for i in jsonfile['data']:
                    if jsonfile['data'][i]:
                        for j in jsonfile['data'][i]:
                            if Path(rf"{path}").parts[6]=='state':
                                top_trans_state.append((country, state, year, quarter, i, j['entityName'], j['metric']['count'], j['metric']['amount']))
                            else:
                                top_trans_country.append((country, year, quarter, i, j['entityName'], j['metric']['count'], j['metric']['amount']))

            elif Path(rf"{path}").parts[2]=='top' and Path(rf"{path}").parts[3]=='user':
                top_user.append(file)
                jsonfile = json.load(open(file))    
                country, year, quarter = Path(rf"{path}").parts[5], Path(rf"{path}").parts[-2], (Path(rf"{path}").parts[-1]).strip('.json')
                #state = ""
                if Path(rf"{path}").parts[6]=='state':
                    state = Path(rf"{path}").parts[7]
                for i in jsonfile['data']:
                    if jsonfile['data'][i]:
                        for j in jsonfile['data'][i]:
                            if Path(rf"{path}").parts[6]=='state':
                                top_user_state.append((country, state, year, quarter, i, j['name'], j['registeredUsers']))
                            else:
                                top_user_country.append((country, year, quarter, i, j['name'], j['registeredUsers']))
            
        #print("Aggregated: ",len(aggr_trans))
        #print(len(aggr_trans),len(aggr_user), len(map_trans), len(map_user), len(top_trans), len(top_user))
        #print(top_user_country)
        print(len(agg_trans_country), len(agg_trans_state), len(agg_user_country), len(agg_user_state))
        print(len(map_trans_country), len(map_trans_state), len(map_user_country), len(map_user_state))
        print(len(top_trans_country), len(top_trans_state), len(top_user_country), len(top_user_state))
        
        # Storing the data into MySQL
        conn = db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("USE phonepe")                
                cursor.executemany("INSERT INTO agg_trans_country VALUES(%s, %s, %s, %s, %s, %s)", agg_trans_country)
                cursor.executemany("INSERT INTO agg_trans_state VALUES(%s, %s, %s, %s, %s, %s, %s)", agg_trans_state)
                cursor.executemany("INSERT INTO agg_user_country VALUES(%s, %s, %s, %s, %s, %s)", agg_user_country)
                cursor.executemany("INSERT INTO agg_user_state VALUES(%s, %s, %s, %s, %s, %s, %s)", agg_user_state)
                cursor.executemany("INSERT INTO map_trans_country VALUES(%s, %s, %s, %s, %s, %s)", map_trans_country)
                cursor.executemany("INSERT INTO map_trans_state VALUES(%s, %s, %s, %s, %s, %s, %s)", map_trans_state)
                cursor.executemany("INSERT INTO map_user_country VALUES(%s, %s, %s, %s, %s, %s)", map_user_country)
                cursor.executemany("INSERT INTO map_user_state VALUES(%s, %s, %s, %s, %s, %s, %s)", map_user_state)
                cursor.executemany("INSERT INTO top_trans_country VALUES(%s, %s, %s, %s, %s, %s, %s)", top_trans_country)
                cursor.executemany("INSERT INTO top_trans_state VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", top_trans_state)
                cursor.executemany("INSERT INTO top_user_country VALUES(%s, %s, %s, %s, %s, %s)", top_user_country)
                cursor.executemany("INSERT INTO top_user_state VALUES(%s, %s, %s, %s, %s, %s, %s)", top_user_state)
                conn.commit()   
        except Error as e:
            print("Error during data insertion: ",e)
        conn.close()

def execute_github_data_extraction():
    clone() 
    create_mysqlschema()
    filepaths = read_dir()
    extract_data(filepaths)
    return 1




