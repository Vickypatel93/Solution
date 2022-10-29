import datetime
import os
import pandas as pd
from config import mysql
from sqlalchemy import create_engine
import logFile as lconfig

module_name = "stats.py"
logger = lconfig.get_logger(module_name)

db_connection = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="...", db="wx_data"))
logger.info("Successfully connected with MySql server")

# Load data from database
conn = mysql.connect()
cursor = conn.cursor()

# Get the all table from database
cursor.execute("show tables")

# Stats calculation
result = []
for wf in cursor:
    df =  pd.read_sql('SELECT * FROM '+ wf, con=db_connection) # To extract each table into data frame
    logger.info("Get data from  table {b}".format(b=wf))
    # Filtering the required valid data.
    df = df[(df['precipitation'] != '-9999') & (df['maximum_temp'] != '-9999') & (df['minimu_temp'] != "-9999")]
    df['date'] = map(lambda x: datetime.datetime.fromtimestamp(float(x)), df['date'])
    df['year'] = map(lambda x: df['date'].ix[x].year, df.index)
    df['maximum_temp'] = map(float, df['maximum_temp'].values)
    df['minimu_temp'] = map(float, df['minimu_temp'].values)
    df['precipitation'] = map(float, df['precipitation'].values)
    mean = df.groupby('year').mean()
    total = df.groupby('year').sum()
    maximutemp_mean = mean['maximum_temp']
    minimutemp_mean = mean['minimu_temp']
    total_precipitation = total['precipitation']
    logger.info("Calculated stats !")
    result.append("{0}\t{1}\t{2:.2f}\t{3:.2f}\t{4:.2f}".format(wf,
                                                               total_precipitation.index[0],
                                                               maximutemp_mean.values[0],
                                                               minimutemp_mean.values[0],
                                                               total_precipitation.values[0]))

# Insert whole result DataFrame into MySQL
cursor.execute("create table result (fileName varchar(100), year int, maixmum_temp_mean float, minimum_temp_mean float, total_precipitation float)")
logger.info("Successfully created table {b}".format(b=result))
pd.DataFrame(result, columns=['fileName', 'year', 'maixmum_temp_mean', 'minimum_temp_mean', 'total_precipitation']).to_sql('result', con = db_connection, if_exists = 'append')
logger.info("Successfully inserted data into  table {b}".format(b=result))