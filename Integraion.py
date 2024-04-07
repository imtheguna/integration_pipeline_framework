import glob
import os
from apply_df import DQValidation
from audit_functions import *
from aws_functions import AWSFunctions
from dbinfo import DatabaseConnectionDetails
from commonfunction import CommonFunctions
from filefunction import FileFunction
from loadfunction import TableLoadFunction
import traceback
from constants import *
import io
import pandas as pd

current_user = ''
source_file = ''

auditLog = AuditLog()
logmetadata = Logmetadata()
databaseConnection = DatabaseConnectionDetails()
commonFunctions = CommonFunctions()
fileFunction = FileFunction()
tableLoadFunction = TableLoadFunction()
awsFunctions = AWSFunctions()
dqValidation = DQValidation()
cursor = ''


## creating Source Data
def source(metadata):
    global current_user,source_file
    srcDF = False

    ## Reading the file
    if(metadata['source'].lower()=='file'):
        data = fileFunction.readFile(metadata['src_file_metadata'])
        srcDF = data['data']
        duplicateDF = data['duplicates']
        source_file = data['file']
        ## Ading columns
        if('addcolumns' in metadata['tgt_table_info']):
            srcDF = commonFunctions.addColumns(columns=metadata['tgt_table_info']['addcolumns'],df=srcDF,current_user=current_user,
                               current_file=source_file)
            
        logmetadata.setsrcObj(obj=source_file)
    
    ## Reading from table
    elif(metadata['source'].lower()=='table'):
        ## Reading from PostgresSQL
        if(metadata['src_database_meta_data']['DBType'].lower()=='postgressql'):
            data = tableLoadFunction.readPostgresSQL(dbdata=metadata['src_database_meta_data'],
                                                tabledata=metadata['src_table_info'])
        ## Reading from Teradata
        elif(metadata['src_database_meta_data']['DBType'].lower()=='teradata'):
            data = tableLoadFunction.readPostgresSQL(dbdata=metadata['src_database_meta_data'],
                                                tabledata=metadata['src_table_info'])
        ## Reading from MySQL
        elif(metadata['src_database_meta_data']['DBType'].lower()=='mysql'):
            data = tableLoadFunction.readMySQL(dbdata=metadata['src_database_meta_data'],
                                                tabledata=metadata['src_table_info'])
        ## Reading from RedShift
        elif(metadata['src_database_meta_data']['DBType'].lower()=='redshift'):
            data = tableLoadFunction.readRedShift(dbdata=metadata['src_database_meta_data'],
                                                tabledata=metadata['src_table_info'])
        
        srcDF = data['data']
        duplicateDF = data['duplicates']
        ## Ading columns
        if('addcolumns' in metadata['src_table_info'].keys()):
            srcDF = commonFunctions.addColumns(columns=metadata['src_table_info']['addcolumns'],df=srcDF,current_user=current_user,
                               current_file=source_file)
            
        logmetadata.setsrcObj(obj=metadata['src_table_info']['tablename'])
        

    logmetadata.setImpSrcRowCount(count=len(srcDF))    
    return {'srcdata':srcDF,'duplicates':duplicateDF}

## Debatching
def taget_debatch(srcDF):
    schema = {
        "type": "record",
        "name": "temp",
        "fields": [
            {"name": column, "type": "string"} for column in srcDF.columns
        ]
        }
    for i, chunk in enumerate(pd.read_csv(io.StringIO(srcDF.to_csv(index=False)), chunksize=chunk_size)):
            for idx, record in chunk.iterrows():
                record_dict = {key: str(value) for key, value in record.to_dict().items()}
                avro_filename = f"temp_{idx}.avro"
                fileFunction.writeAvro(row=record_dict,filepath=avro_temp_path+avro_filename,schema=schema)
    
    file_pattern = os.path.join(avro_temp_path+'*avro')
    matching_files = glob.glob(file_pattern) 
    for file in matching_files:
        avrodata = fileFunction.readAvro(avro_file=file,returnDict=True)
        awsFunctions.mappedSQS(avrodata=avrodata,avropath=file)
    logmetadata.setTgtObj(obj='Debatching')
    logmetadata.setImpTgtRowCount(count=len(srcDF))
        


## Writing the target
def target_file_table(metadata,srcDf):

    ## writing table
    if(metadata['target'].lower()=='table'):
        ## PostgresSQL
        if(metadata['tgt_database_meta_data']['DBType'].lower()=='postgressql'):
           count = tableLoadFunction.loadPostgresSQL(df=srcDf,dbdata=metadata['tgt_database_meta_data'],
                                              loadtype=metadata['tgt_table_info']['loadtype'],
                                              tabledata=metadata['tgt_table_info'])
        ## Teradata   
        elif(metadata['tgt_database_meta_data']['DBType'].lower()=='teradata'):
            count = tableLoadFunction.loadTeradata(df=srcDf,dbdata=metadata['tgt_database_meta_data'],
                                              tabledata=metadata['tgt_table_info'])
        ## MySQL   
        elif(metadata['tgt_database_meta_data']['DBType'].lower()=='mysql'):
            count = tableLoadFunction.loadMySQL(df=srcDf,dbdata=metadata['tgt_database_meta_data'],
                                              tabledata=metadata['tgt_table_info'])
        ## RedShift
        elif(metadata['tgt_database_meta_data']['DBType'].lower()=='redshift'):
            count = tableLoadFunction.loadRedShift(df=srcDf,dbdata=metadata['tgt_database_meta_data'],
                                              tabledata=metadata['tgt_table_info'])
            
        logmetadata.setTgtObj(obj=metadata['tgt_table_info']['tablename'])
        logmetadata.setImpTgtRowCount(count=count)

    ## writing files
    elif(metadata['target'].lower()=='file'):
        if(metadata['tgt_file_metadata']['filetype'] in ['csv','text','psv']):
            fileFunction.writeFile(srcDF,filedata=metadata['tgt_file_metadata'],type='delimiter')
        elif(metadata['tgt_file_metadata']['filetype'] in ['json']):
            fileFunction.writeFile(srcDF,filedata=metadata['tgt_file_metadata'],type='json')
            
        logmetadata.setTgtObj(obj=metadata['tgt_file_metadata']['filename'])


try:
    
    commonFunctions.cleanup(folder_path=temp_path)
    commonFunctions.cleanup(folder_path=avro_temp_path)
    logging(logtype=Log.info,msg='Job Started')

    ## Log metadata creating
    logmetadata.setProcessStartTime(commonFunctions.getCurrentDateTime('%Y-%m-%d %H:%M:%S'))
    logmetadata.setLogPath(path=log_path)

    logging(logtype=Log.info,msg='Reading Secrets')
    ## Read Password from Secrets
    password = awsFunctions.readSecrets(region_name='us-east-1',secret_name='pg_password')
    ## Read Meta Data.
    cursor = databaseConnection.connectPostgresSQL(endpoint='localhost',port=5432,database='postgres',
                                            user='postgres',password=password,schema='public')
    
    logging(logtype=Log.info,msg='Database connected')

    ## faced on the load name, we will read the data from DB and process the load.
    load_name = 'a_s3'
    
    cursor.execute(f"select current_user")
    data = cursor.fetchone()
    current_user = data[0]


    cursor.execute(f"select * from etl_load_drvr where name='{load_name}'")
    data = cursor.fetchall()
    
    ## if we have Multiple entries for same name then the process will end.
    if(len(data)>1):
        print(f'Multiple entries for name {load_name}')
        logging(logtype=Log.error,msg=f'Multiple entries for name {load_name}')
        exit(1)

    ## Terminate process if no data for given name.
    elif(len(data)==0):
        logging(logtype=Log.error,msg=f'No entries for name {load_name}')
        print(f'No entries for name {load_name}')
        exit(0)

    ## Create records
    metadata = commonFunctions.create_record(data[0],[desc[0] for desc in cursor.description])

    # Validating the metadata
    validateMsg  = commonFunctions.validateMeta(metadata)

    if(validateMsg!=True):
        logging(logtype=Log.error,msg=validateMsg)
        raise ValueError(validateMsg)

    logmetadata.setprocesstype(type=metadata['loadtype'])
    logmetadata.setProcessName(name=metadata['name'])
    logmetadata.setProcessKey(key=metadata['etl_load_drvr_key'])

    logging(logtype=Log.info,msg='Reading Source')
    ## Reading Source
    srcData =  source(metadata=metadata)
    srcDF = srcData['srcdata']
    duplicateDf = srcData['duplicates']

    if(metadata['dq_screen_id'] is None or metadata['dq_screen_id']==''):
        logging(logtype=Log.info,msg='No DQ Screen for this flow')
    else:
        logging(logtype=Log.info,msg='DQ Metadata Validation Started')
        dqValidation.readMetadata(screen_id=metadata['dq_screen_id'],cursor=cursor)
        if(not dqValidation.validatingMetadata()):
            logging(logtype=Log.info,msg='DQ Setup Not Valid')
        else:
            filtered_df = dqValidation.applyRule(data=srcDF)
            ## Filtering Issue Records
            if(filtered_df is not None and len(filtered_df)!=0):
                srcDF = filtered_df
        


    if(metadata['loadtype'] in ['filetotable','tabletofile','tabletotable','filetofile']):
        logging(logtype=Log.info,msg='Apply target')
        ## Writing the target
        target_file_table(metadata=metadata,srcDf=srcDF)

    elif(metadata['loadtype'] in ['debatch']):
        taget_debatch(srcDF=srcDF)
    
    logmetadata.setProcessEndTime(commonFunctions.getCurrentDateTime('%Y-%m-%d %H:%M:%S'))
    logmetadata.setStatus(status='Success')

    logging(logtype=Log.info,msg='Completed')

except Exception as e:
    error = traceback.format_exc()
    print(error)
    logging(logtype=Log.error,msg=error)
    logmetadata.setError(error=str(e))
    logmetadata.setStatus(status='Failed')
    logmetadata.setProcessEndTime(commonFunctions.getCurrentDateTime('%Y-%m-%d %H:%M:%S'))

    awsFunctions.sendMail(toMail='GUNATWIN6@gmail.com',fromMail='GUNATWIN6@gmail.com',sub='The Pipeline got failed',
                                  body=f"""Hi,\n \n The {load_name} pipeline got failed with error - {error} \n \n Time - {commonFunctions.getCurrentDateTime('%Y-%m-%d %H:%M:%S')}
""",AWS_REGION='us-east-1')

finally:
    auditLog.updateAduitTable(metadata=logmetadata.getMetadata())
    

