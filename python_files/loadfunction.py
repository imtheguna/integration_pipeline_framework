from io import StringIO
from aws_functions import AWSFunctions
from dbinfo import DatabaseConnectionDetails
import pandas as pd
import numpy as np
import mysql.connector

databaseConnection = DatabaseConnectionDetails ()
awsFunctions = AWSFunctions()

class TableLoadFunction:
    def __init__(self) -> None:
        pass
    
    ################################### Teradata ###################################
    ## Read data from Teradata
    def readTeradata(self,dbdata,tabledata):
        if(dbdata['password_type']=='secret'):
            password = awsFunctions.readSecrets(region_name='us-east-1',secret_name=dbdata['secret_name'])
        else:
            password = dbdata['password']

        cursor = databaseConnection.connectTD(hostname=dbdata['hostname'],drivername='Teradata atabase ODBC Driver 16.20',
                                            uid=dbdata['user'],pwd=password)
        cursor.execute(tabledata['query'])
        rows = cursor.fetchall()

        # Get the column names
        colnames = [desc[0] for desc in cursor.description]

        # Create DataFrame from fetched data
        df = pd.DataFrame(rows, columns=colnames)
        
        duplicates = df[df.duplicated()]

        df = df.drop_duplicates()

        return dict({'data' : df,'duplicates':duplicates})
    

    ## Load data into Teradata
    def loadTeradata(self,df,dbdata,tabledata):
        if(dbdata['password_type']=='secret'):
            password = awsFunctions.readSecrets(region_name='us-east-1',secret_name=dbdata['secret_name'])
        else:
            password = dbdata['password']

        cursor = databaseConnection.connectTD(hostname=dbdata['hostname'],drivername='Teradata atabase ODBC Driver 16.20',
                                            uid=dbdata['user'],pwd=password)
        count = 0 
        for _,row in df.iterrows():
            query = f"insert into {tabledata['tablename']} values ({', '.join['?'] * len(row)})"
            cursor.execute(query)
            count+=count
        return count

    ################################### PostgresSQL ###################################
    ## Load data into PostgresSQL
    def loadPostgresSQL(self,df,dbdata,tabledata,loadtype='copy'):

        if(dbdata['password_type']=='secret'):
            password = awsFunctions.readSecrets(region_name='us-east-1',secret_name=dbdata['secret_name'])
        else:
            password = dbdata['password']

        if(loadtype=='copy'):
            cursor = databaseConnection.connectPostgresSQL(endpoint=dbdata['hostname'],port=dbdata['port'],
                                                           database=dbdata['database'],
                                            user=dbdata['user'],password=password,schema=dbdata['schema'])
            csv_data = StringIO()
            df.to_csv(csv_data, index=False, header=False)
            csv_data.seek(0)
            copy_query = f"COPY {tabledata['tablename']} FROM STDIN WITH CSV ENCODING '{tabledata['encoding']}'"
            cursor.copy_expert(sql=copy_query, file=csv_data)
            
            return len(df)

        elif(loadtype=='insert'):
            columns = list(df.columns)
            cursor = databaseConnection.connectPostgresSQL(endpoint=dbdata['hostname'],port=dbdata['port'],
                                                           database=dbdata['database'],
                                            user=dbdata['user'],password=password,schema=dbdata['schema'])
        
            insert_statement = "INSERT INTO {} ({}) VALUES ({})"
            insert_columns = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_statement = insert_statement.format(tabledata['tablename'],insert_columns, placeholders)

            count = 0 
            for _, row in df.iterrows():
                cursor.execute(insert_statement, tuple(row))
                count+=count
            return count

    ## Read data from PostgresSQL
    def readPostgresSQL(self,dbdata,tabledata):
        
        if(dbdata['password_type']=='secret'):
            password = awsFunctions.readSecrets(region_name='us-east-1',secret_name=dbdata['secret_name'])
        else:
            password = dbdata['password']

        cursor = databaseConnection.connectPostgresSQL(endpoint=dbdata['hostname'],port=dbdata['port'],
                                                           database=dbdata['database'],
                                            user=dbdata['user'],password=password,schema=dbdata['schema'])
        cursor.execute(tabledata['query'])
        rows = cursor.fetchall()

        # Get the column names
        colnames = [desc[0] for desc in cursor.description]

        # Create DataFrame from fetched data
        df = pd.DataFrame(np.array(rows), columns=colnames)

        duplicates = df[df.duplicated()]
        
        df = df.drop_duplicates()

        return dict({'data' : df,'duplicates':duplicates})
    
    ################################### MySQL ###################################
    ## Load data into MySQL
    def loadMySQL(self,df,dbdata,tabledata):
       
        if(dbdata['password_type']=='secret'):
            password = awsFunctions.readSecrets(region_name='us-east-1',secret_name=dbdata['secret_name'])
        else:
            password = dbdata['password']

        conn = databaseConnection.connectMySQL(dbname=dbdata['database'],
                                                    uid=dbdata['user'],
                                                    pwd=password,
                                                    server=dbdata['hostname'])
        cursor = conn.cursor()
        count = 0 
        for _, row in df.iterrows():
            values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
            insert_sql = f"INSERT INTO {tabledata['tablename']} VALUES ({values})"
            cursor.execute(insert_sql)
            count+=count
        return count
        
    ## Read data from MySQL
    def readMySQL(self,dbdata,tabledata):
        
        if(dbdata['password_type']=='secret'):
            password = awsFunctions.readSecrets(region_name='us-east-1',secret_name=dbdata['secret_name'])
        else:
            password = dbdata['password']

        conn = databaseConnection.connectMySQL(dbname=dbdata['database'],
                                                    uid=dbdata['user'],
                                                    pwd=password,
                                                    server=dbdata['hostname'])
        cursor = conn.cursor()

        cursor.execute(tabledata['query'])
        rows = cursor.fetchall()

        # Get the column names
        colnames = [desc[0] for desc in cursor.description]

        # Create DataFrame from fetched data
        df = pd.DataFrame(rows, columns=colnames)

        duplicates = df[df.duplicated()]
        
        df = df.drop_duplicates()

        return dict({'data' : df,'duplicates':duplicates})
    
    ################################### RedShift ###################################
    ## Load data into RedShift
    def loadRedShift(self,df,dbdata,tabledata):
        
        if(dbdata['password_type']=='secret'):
            password = awsFunctions.readSecrets(region_name='us-east-1',secret_name=dbdata['secret_name'])
        else:
            password = dbdata['password']

        cursor = databaseConnection.connectRedShift(host=dbdata['hostname'],user=dbdata['user'],password=password,port=dbdata['port'],
                                                    database=dbdata['database'])
        
        count = 0 
        for _, row in df.iterrows():
            values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
            insert_query = f"INSERT INTO {tabledata['tablename']} VALUES ({values})"
            cursor.execute(insert_query)
            count+=count
        return count
    
    ## Read data from RedShift
    def readRedShift(self,dbdata,tabledata):
        
        if(dbdata['password_type']=='secret'):
            password = awsFunctions.readSecrets(region_name='us-east-1',secret_name=dbdata['secret_name'])
        else:
            password = dbdata['password']

        cursor = databaseConnection.connectRedShift(host=dbdata['hostname'],user=dbdata['user'],password=password,port=dbdata['port'],
                                                    database=dbdata['database'])
        
        cursor.execute(tabledata['query'])
        rows = cursor.fetchall()

        # Get the column names
        colnames = [desc[0] for desc in cursor.description]

        # Create DataFrame from fetched data
        df = pd.DataFrame(np.array(rows), columns=colnames)

        duplicates = df[df.duplicated()]
        
        df = df.drop_duplicates()

        return dict({'data' : df,'duplicates':duplicates})