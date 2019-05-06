# ================================================================================================================================
#
#       AUXILIARY FUNCTIONS FOR DATABASE HANDLING - ALGORITHM 4 (INCIDENT REPORTS)
#

import pymysql as psql

import pandas as pd
from pandas import *

# -------------------------------------------------------------------------------------------------------------------
# Function to insert a new tuple from Incident Report to database table 'Flood_CRCL_DB'. If a table Incident_Reports`
# does not exist, it is created and then the new tuple is inserted.
#
def insert_IncReport_to_db(inpt_rec):
    try:
        connection = psql.connect(host='localhost', user='root', password='', db='Flood_CRCL_DB')
    except:
        print("No connection!!! No insert data action was performed!!")
        return

    try:
        with connection.cursor() as cursor:

            # Check if Table `Incident_Reports` exists
            stmt = "SHOW TABLES LIKE 'Incident_Reports'"
            cursor.execute(stmt)
            result = cursor.fetchone()

            if not result:
                # Create table named "tableName"
                print("\n There are no table named Incident_Reports. Create one right now ... ")
                db_name = "Flood_CRCL_DB"
                table_name = "`Incident_Reports`"

                dict_entries = [
                    {'colname': "`Incident_ID`", 'type': 'VARCHAR(255)', 'check_null': 'NOT NULL', 'PRIMARY KEY': True},
                    {'colname': "`Category_field`", 'type': 'VARCHAR(10)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Exposure_cultural_asset`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Exposure_field_1`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Exposure_field_2`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Exposure_people`", 'type': 'DOUBLE', 'check_null': 'NOT NULL', 'PRIMARY KEY': False},
                    {'colname': "`Hazard_field`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Hazard_value`", 'type': 'DOUBLE', 'check_null': 'NOT NULL', 'PRIMARY KEY': False},
                    {'colname': "`Hazard_description`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Hydraulic_Risk`", 'type': 'DOUBLE', 'check_null': 'NOT NULL', 'PRIMARY KEY': False},
                    {'colname': "`Incident_Latitude`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Incident_Longitude`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Incident_DateTime`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Severity`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL', 'PRIMARY KEY': False},
                    {'colname': "`Vulnerability_cultural_asset`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Vulnerability_economic_act`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Vulnerability_people`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Time_Step_1`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Time_Step_2`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Total_Time`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False}
                ]

                create_table(db_name, table_name, dict_entries)

            #===================================================================================
            # The table named "tableName" exists to DB, so insert into it the inpt_rec
            print(" Insert inpt_rec into Incident_Reports table. ")

            # Create a new record
            sql1 = "INSERT IGNORE INTO `Incident_Reports` ( `Incident_ID`, `Category_field`, \
            `Exposure_cultural_asset`, `Exposure_field_1`, `Exposure_field_2`, \
            `Exposure_people`, `Hazard_field`, `Hazard_value`, `Hazard_description`, \
            `Hydraulic_Risk`, `Incident_Latitude`, `Incident_Longitude`, "
            sql2 = "`Incident_DateTime`, `Severity`, `Vulnerability_cultural_asset`, "
            sql3 = "`Vulnerability_economic_act`, `Vulnerability_people` , "
            sql4 = "`Time_Step_1`, `Time_Step_2`, `Total_Time` )"
            sql5 = "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

            sql = sql1 + sql2 + sql3 + sql4 + sql5

            cursor.execute(sql, (inpt_rec['Incident_ID'], inpt_rec['Category_field'],
                                 float(inpt_rec['Exposure_cultural_asset']),
                                 inpt_rec['Exposure_field'][0],
                                 inpt_rec['Exposure_field'][1],
                                 float(inpt_rec['Exposure_people']),
                                 inpt_rec['Hazard_field'],
                                 float(inpt_rec['Hazard_value']['Value']),
                                 inpt_rec['Hazard_value']['Description'],
                                 float(inpt_rec['Hydraulic_Risk']),
                                 float(inpt_rec['Inc_Lat']),
                                 float(inpt_rec['Inc_Long']),
                                 inpt_rec['Incident_DateTime'],
                                 inpt_rec['Severity'],
                                 float(inpt_rec['Vulnerability_cultural_asset']),
                                 float(inpt_rec['Vulnerability_economic_act']),
                                 float(inpt_rec['Vulnerability_people']),
                                 float(inpt_rec['Time_Step_1']),
                                 float(inpt_rec['Time_Step_2']),
                                 float(inpt_rec['Total_Time'])
                                 ))

            # connection is not autocommit by default. So you must commit to save your changes.
            connection.commit()

    finally:
        connection.close()

# ------------------------------------------------------------------------------------------------------------
# Function to insert into a table Total_Risk_Assessment the calculations for the accumulated Risk Assessment
# over all the Incidents which are taken place until the current date/time. The estimation of the Risk was
# carried out using two methods: Voting (Vot) and Generalised Mean (GM).
# If the table does not exist, it is created first and then the results are stored.
#
def insert_Risk_Assessment_to_db( risk_assess_tuple ):
    try:
        connection = psql.connect(host='localhost', user='root', password='', db='Flood_CRCL_DB')
    except:
        print("No connection!!! No insert data action was performed!!")
        return

    try:
        with connection.cursor() as cursor:

            # Check if Table `Total_Risk_Assessment` exists
            stmt = "SHOW TABLES LIKE 'Total_Risk_Assessment'"
            cursor.execute(stmt)
            result = cursor.fetchone()

            # Create table Total_Risk_Assessment
            if not result:

                print("\n There are no table named Total_Risk_Assessment. Create one right now ... ")
                db_name = "Flood_CRCL_DB"
                table_name = "`Total_Risk_Assessment`"

                dict_entries = [
                    {'colname': "`Incident_ID`", 'type': 'VARCHAR(255)', 'check_null': 'NOT NULL', 'PRIMARY KEY': True},
                    {'colname': "`Incident_DateTime`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Incident_Latitude`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Incident_Longitude`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Total_Risk_Assessment_Vot`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Total_Severity_Vot`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Total_Category_Vot`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Total_Risk_Assessment_GM`", 'type': 'DOUBLE', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Total_Severity_GM`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False},
                    {'colname': "`Total_Category_GM`", 'type': 'VARCHAR(50)', 'check_null': 'NOT NULL',
                     'PRIMARY KEY': False}
                ]

                create_table(db_name, table_name, dict_entries)

            # ===================================================================================
            # The table named "Total_Risk_Assessment" exists, so append the new tuple
            print(" Insert risk_assess_tuple into  Total_Risk_Assessment table. ")

            # Create a new record
            sql1 = "INSERT IGNORE INTO `Total_Risk_Assessment` ( `Incident_ID`, `Incident_DateTime`, \
                   `Incident_Latitude`, `Incident_Longitude`, `Total_Risk_Assessment_Vot`,\
                   `Total_Severity_Vot`, `Total_Category_Vot`, `Total_Risk_Assessment_GM`, \
                   `Total_Severity_GM`, `Total_Category_GM` )"
            sql2 = "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

            sql = sql1 + sql2

            cursor.execute(sql, (risk_assess_tuple['Incident_ID'],
                                 risk_assess_tuple['Incident_DateTime'],
                                 float(risk_assess_tuple['Inc_Lat']),
                                 float(risk_assess_tuple['Inc_Long']),
                                 float(risk_assess_tuple['Total_Risk_Assessment_Vot']),
                                 risk_assess_tuple['Total_Severity_Vot'],
                                 risk_assess_tuple['Total_Category_Vot'],
                                 float(risk_assess_tuple['Total_Risk_Assessment_GM']),
                                 risk_assess_tuple['Total_Severity_GM'],
                                 risk_assess_tuple['Total_Category_GM']
                                 )
                           )

            # connection is not autocommit by default. So you must commit to save your changes.
            connection.commit()
    finally:
        connection.close()


# ------------------------------------------------------------------------------------------------------------
# Function to extract tuples from database table (i.e. 'Incident_Reports')
#

def extract_IncReports_from_db( db_name, table_name ):

    conn = psql.connect(host='localhost', user='root', password='', db=db_name)
    a = conn.cursor()

    sql = 'Select * from ' + table_name
    num_of_rows = a.execute(sql)

    # Fetch all the rows
    data = a.fetchall()

    colnames = ['Incident_ID', 'Category_field', 'Exposure_cultural_asset', 'Exposure_field_1', 'Exposure_field_2',
                'Exposure_people', 'Hazard_field', 'Hazard_value', 'Hazard_description',
                'Hydraulic_Risk', 'Incident_Latitude', 'Incident_Longitude', 'Incident_DateTime',
                'Severity', 'Vulnerability_cultural_asset', 'Vulnerability_economic_act', 'Vulnerability_people',
                'Time_Step_1', 'Time_Step_2', 'Total_Time' ]

    df = pd.DataFrame(list(data), columns=colnames)

    return(df)

# -------------------------------------------------------------------------------------------------------------------
# For Pre-Emergency Phase, with this function the interest river sections are loaded to the table in DB
#
def load_data_file(db_name, table_name, filename):

    conn = psql.connect(host='localhost', user='root', password='', db=db_name)

    sql = "LOAD DATA INFILE " + filename + " INTO TABLE " + table_name + " FIELDS TERMINATED BY ','" + " IGNORE 1 LINES;"

    curs = conn.cursor()
    curs.execute(sql)

    conn.commit()
    conn.close()

# -------------------------------------------------------------------------------------------------------------------
# For Pre-Emergency Phase, with this function the interest river sections are loaded to the table in DB
#
# Created by John
#
def load_data_from_file(db_name, table_name, filename):
    conn = psql.connect(host='localhost', user='root', password='', db=db_name)

    sql = "LOAD DATA INFILE " + filename + " INTO TABLE " + table_name + " FIELDS TERMINATED BY ','" + " IGNORE 1 LINES;"
    # print(sql)

    curs = conn.cursor()
    curs.execute(sql)

    # Check if table has entries
    Data = 'SELECT * FROM ' + table_name
    num_of_rows = curs.execute(Data)

    # Check if csv has entries and check if there are the same as the loaded data
    csv_fname = filename.split("'")[1]
    csv_frame = read_csv(csv_fname, sep=",")
    num_of_csv_rows = len(csv_frame)

    if num_of_rows == num_of_csv_rows:
        print("Data Fully Loaded from CSV")
    else:
        print("Data didnt Fully Loaded from CSV ")

    conn.commit()
    conn.close()

#==================================================================================================================
#
def create_table(db_name, table_name, dict_entries):
    conn = psql.connect(host='localhost', user='root', password='', db=db_name)

    parameters = ""
    for i, it in enumerate(dict_entries):

        parameters = parameters + " " + str(it['colname']) + " " + str(it['type']) + " " + str(it['check_null'])
        if it['PRIMARY KEY'] == True:
            parameters = parameters + " " + 'PRIMARY KEY'

        if i < (len(dict_entries) - 1):
            parameters = parameters + ","

    sql = "CREATE TABLE" + " " + db_name + "." + table_name + "(" + parameters + ")" + " " + "ENGINE = InnoDB;"

    # print(sql)

    curs = conn.cursor()
    curs.execute(sql)

    conn.commit()
    conn.close()