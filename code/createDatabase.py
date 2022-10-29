import mysql.connector
import pandas as pd
import os
import logFile as lconfig

module_name = "createDatabase.py"
logger = lconfig.get_logger(module_name)

path = '../data'
list_of_files = os.listdir(path)

# To process txt file and return the corresponding data frame.
def get_dataframe(file_path, ele, element):
    if element == "yld_data":
        columns=['year', 'quantity']
    if element == "wx_data":
        columns = ['_date', 'maximum_temp', 'minimu_temp', 'precipitation']
    data = open(file_path+'/'+ele).readlines()
    file_data = []
    for row in data:
        file_data.append(dict(zip(columns,[i.strip('\n') for i in row.split('\t')])))
    dataframe = pd.DataFrame(file_data)
    return dataframe

mydb = mysql.connector.connect(host='localhost',
                               user='root',
                               password='...')
logger.info("Successfully connected with MySql server")
# Creating a database
mycursor = mydb.cursor()
for element in list_of_files:
    mycursor.execute("create database %s" % element)
    logger.info("Successfully created Database {a}".format(element))

    # Connect to the database
    mydatabase = mysql.connector.connect(host='localhost',
                                         database="{}".format(element),
                                         user='root',
                                         password='...')
    logger.info("Successfully connected to Database {a}".format(element))
    mydbcursor = mydatabase.cursor()
    if element == "yld_data":
        datadir = os.listdir(os.path.join(path, element))
        for ele in datadir:
            mydbcursor.execute("""create table {}
                                (year int, quantity float)""".format(ele.split('.')[0]))
            logger.info("Successfully created table {b} in Database {a}".format(a = element, b = ele.split('.')[0]))
            filePath = os.path.join(path, element)
            df = get_dataframe(filePath, ele, element)
            # To remove duplicate entries
            df.drop_duplicates(keep='first', inplace=True )
            for _, row in df.iterrows():
                sqlQuery = "insert into {} (year , quantity) values (%s, %s)".format(ele.split('.')[0])
                print(row)
                mydbcursor.execute(sqlQuery, tuple(row))
                mydatabase.commit()
                logger.info("Successfully inserted data into  table {b}".format(b=ele.split('.')[0]))


    if element == "wx_data":
        datadir = os.listdir(os.path.join(path, element))
        for ele in datadir:
            mydbcursor.execute("""create table {}
                                            (_date date, maximum_temp float, minimu_temp float, precipitation float)""".format(
                ele.split('.')[0]))
            logger.info("Successfully created table {b} in Database {a}".format(a=element, b=ele.split('.')[0]))
            filePath = os.path.join(path, element)
            df = get_dataframe(filePath, ele, element)
            # To remove duplicate entries
            df.drop_duplicates(keep='first', inplace=True)
            for _, row in df.iterrows():
                sqlQuery = "insert into {} (_date , maximum_temp, minimu_temp, precipitation) values (%s, %s, %s, %s)".format(ele.split('.')[0])
                print(row)
                mydbcursor.execute(sqlQuery, tuple(row))
                mydatabase.commit()
                logger.info("Successfully inserted data into  table {b}".format(b=ele.split('.')[0]))