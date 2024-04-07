from aws_functions import AWSFunctions
from commonfunction import CommonFunctions
from dbinfo import DatabaseConnectionDetails
from enum import Enum
from constants import *

commonFunctions = CommonFunctions()
awsFunctions = AWSFunctions()
databaseConnection = DatabaseConnectionDetails()

class Logmetadata:
    def __init__(self) -> None:
        self.current_date = commonFunctions.getCurrentDateTime('%Y-%m-%d')
        self.processkey = None
        self.processname = None
        self.processStrattime = None
        self.processEndtime = None
        self.srcObj = None
        self.tgtObj = None
        self.srccount = None
        self.tgtcount = None
        self.error = None
        self.logpath = None
        self.status = None
        self.processtype = None
    def setProcessKey(self,key):
        self.processkey = key
    def setProcessName(self,name):
        self.processname = name
    def setProcessStartTime(self,time):
        self.processStrattime = time
    def setProcessEndTime(self,time):
        self.processEndtime = time
    def setTgtObj(self,obj):
        self.tgtObj = obj
    def setsrcObj(self,obj):
        self.srcObj = obj
    def setImpSrcRowCount(self,count):
        self.srccount = count
    def setImpTgtRowCount(self,count):
        self.tgtcount = count
    def setError(self,error):
        self.error = error
    def setLogPath(self,path):
        self.logpath = path
    def setStatus(self,status):
        self.status = status
    def setprocesstype(self,type):
        self.processtype = type

    def getMetadata(self):
        return {
            'process_key':self.processkey,
            'process_name':self.processname,
            'start_time':self.processStrattime,
            'end_time':self.processEndtime,
            'tgt_object':self.tgtObj,
            'src_object':self.srcObj,
            'src_row_count':self.srccount,
            'tgt_row_count':self.tgtcount,
            'run_date':self.current_date,
            'error':self.error,
            'log_path':self.logpath,
            'status':self.status,
            'process_type':self.processtype
        } 

class AuditLog:
    def __init__(self) -> None:
        pass
    def updateAduitTable(self,metadata):
        query = """INSERT INTO public.audit_log(
	etl_load_drvr_key, process_type, process_name, start_time, end_time, tgt_object, src_object, src_row_count, tgt_row_count, run_date, error, log_path, status)
	VALUES ('{etl_load_drvr_key}', '{process_type}',' {process_name}','{start_time}','{end_time}','{tgt_object}','{src_object}',
    '{src_row_count}','{tgt_row_count}','{run_date}','{error}','{log_path}','{status}')""".format(
        etl_load_drvr_key=metadata['process_key'],process_type=metadata['process_type']
        ,process_name=metadata['process_name'],start_time=metadata['start_time']
        ,end_time=metadata['end_time'],tgt_object=metadata['tgt_object']
        ,src_object=metadata['src_object'] ,src_row_count=metadata['src_row_count']
         ,tgt_row_count=metadata['tgt_row_count'] ,run_date=metadata['run_date']
          ,error=str(metadata['error']).replace("'",'') ,log_path=metadata['log_path']
           ,status=metadata['status']
    )
        ## Read Password from Secrets
        password = awsFunctions.readSecrets(region_name='us-east-1',secret_name='pg_password')
        ## Read Meta Data.
        cursor = databaseConnection.connectPostgresSQL(endpoint='localhost',port=5432,database='postgres',
                                                user='postgres',password=password,schema='public')
        cursor.execute(query=query)
        print('Completed')

class Log (Enum):
    error = 'Error'
    info = 'Info'
    

def logging(logtype,msg):
    with open(log_path,'a') as file:
        file.write(str(logtype.value)+" : "+msg+"\n")

        




    