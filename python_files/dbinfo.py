import pandas as pd
import pyodbc
import mysql.connector
import psycopg2 as pg
import redshift_connector

class DatabaseConnectionDetails:
    def __init__(self,autocommit=True) -> None:
        self.autocommit = autocommit

    ## Connection for Teradata
    def connectTD(self,drivername,uid,hostname,pwd):
        link = 'DRIVER={DRIVERNAME};DBCNAME={hostname};UID={uid};PWD={pwd}'.format(
                  DRIVERNAME=drivername,hostname=hostname,
                  uid=uid, pwd=pwd)
        cnxn = pyodbc.connect(link,autocommit=self.autocommit)
        cursor = cnxn.cursor()
        return cursor
    
    ## Connection for MySql
    def connectMySQL(self,dbname,uid,pwd,server):
        config = {
                    'host': server,
                    'database': dbname,
                    'user': uid,
                    'password': pwd
                 }
        conn = mysql.connector.connect(**config)
        if(self.autocommit):
            conn.autocommit = True
        return conn
    
    ## Connection for PostgresSQL
    def connectPostgresSQL(self,endpoint,port,database,user,password,schema):
        conn = pg.connect(host=endpoint, port=port, database=database, user=user, password=password,options=f"-c search_path={schema}", sslrootcert="SSLCERTIFICATE")
        cursor = conn.cursor()
        if(self.autocommit):
            conn.autocommit = True
        return cursor
    
    ## Connection for RedShift
    def connectRedShift(self,host,database,user,password,port):
        conn = redshift_connector.connect(
         host=host,
         database=database,
         user=user,
         password=password,
         port=port
        )
        cursor = conn.cursor()
        if(self.autocommit):
            conn.autocommit = True
        return cursor
    
# c = DatabaseConnectionDetails()

# cursor = c.connectRedShift(host='demo.243496286433.us-east-1.redshift-serverless.amazonaws.com',database='dev',user='admin',password='Csg00109')
# row = cursor.execute('select * from test')
# print(row.fetchall())