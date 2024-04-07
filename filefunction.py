import json
import pandas as pd
from commonfunction import CommonFunctions
import glob
import os
from audit_functions import *
from constants import *
import shutil
import fastavro
from aws_functions import AWSFunctions

awsFunctions = AWSFunctions()
out={}

class FileFunction:
    def __init__(self) -> None:
        self.commonFunctions = CommonFunctions()
    
    def flatten_json(self,data):
        output=[]
        global out, JsonData
        def flatten(x,name=''):
            global out
            if type(x) is dict:
                for a in x:
                    flatten(x[a],name+a+'_')
            elif type(x) is list:
                i=0
                for a in x:
                    flatten(a,name+str(i)+'_')
                    i+=1
            else:
                out[name[:-1]]=x
            
        if type(data) is not list:
            data = [data]
        for i in data:
            flatten(i)
            output.append(out)
            out={}

        column_list = output[0].keys()
        
        return output
    
    ## Reading the file, now support test,csv and psv
    def readFile(self,data):
        global temp_path
        if(data['filetype'] not in ['text','csv','psv','parquet','json']):
            raise ValueError('File type not supported') 
        
        ## Reading files from s3
        if(data['source']=='S3'):
            awsFunctions.download_files_from_s3(bucket_name=data['bucket_name'],string_match=data['s3_path'],local_directory=temp_path)
        else:
            temp_path = data['filepath']
        file_pattern = os.path.join(temp_path+data['filename'])
        matching_files = glob.glob(file_pattern)
        
        if(len(matching_files)==0):
            raise ValueError(f"No files found {data['filename']}")
        elif(len(matching_files)>1):
            raise ValueError(f"Too many file for {data['filename']}")
                                                  
        filename = matching_files[0]

        if(data['filetype'] in ['text','csv','psv']):
            ## Default Header
            if('header' in data.keys() and self.commonFunctions.getBool(data['header'])):
                df = pd.read_csv(filename,header=0,sep=data['delimiter'])
            
            ## Custom Header
            elif(data['customHeader'] not in ['',None]):
                df = pd.read_csv(filename,names=data['customHeader'].split(','),sep=data['delimiter'])
            
            ## Default Header
            else:
                df = pd.read_csv(filename,sep=data['delimiter'])
        elif(data['filetype'] in ['parquet']):

            df = pd.read_parquet(filename)
        
        elif(data['filetype'] in ['json']):
            j = json.load(open(filename))
            json_data = self.flatten_json(data=j)
            df = pd.DataFrame(json_data,index=[0])
            if(data['customHeader'] not in ['',None]):
                df.columns = data['customHeader'].split(',')
            print(df)
        
        duplicates = df[df.duplicated()]
        df = df.drop_duplicates()
    
        if('archive_path' in dict(data).keys()):
            if(data['source']=='S3'):
                awsFunctions.archive_files(bucket_name=data['bucket_name'],source_file=filename.split('/')[-1],source_path=data['s3_path'],
                                           archive=data['archive_path'])
            else:
                shutil.move(filename, data['archive_path'])

        return dict({'data' : df,'file':filename.split('/')[-1],'duplicates':duplicates})
    
    ## writing the file. now support text,csv,psv and json
    def writeFile(self,df,filedata,type):
        isFileCreated = False
        if(type=='delimiter'):

            ## without header
            if(filedata['header'].lower()=='no'):
                custom_headers = None
             ## Custom Header
            elif(filedata['customHeader'] not in ['',None]):
                custom_headers = filedata['customHeader'].split(',')
            ## Default Header
            else:
                custom_headers = df.columns.tolist()
            tempfilepath = temp_path+filedata['filename']
            

            if('YYYYMMDD' in tempfilepath):
                date_value = CommonFunctions().getCurrentDateTime('%Y%m%d')
                tempfilepath = str(tempfilepath).replace('YYYYMMDD',date_value)
            elif('YYYYMMDD_HHMMSS' in tempfilepath):
                date_value = CommonFunctions().getCurrentDateTime('%Y%m%d_%H%M%S')
                tempfilepath = tempfilepath.replace('YYYYMMDD_HHMMSS',date_value)

            ## writing the file.
            df.to_csv(tempfilepath,header=custom_headers, index=False,sep=filedata['delimiter'])
            isFileCreated=True
        ## writing the json file
        elif(type=='json'):
            df.to_json(tempfilepath,orient='records')
            isFileCreated=True

        elif(type=='parquet'):
            df.to_parquet(tempfilepath)
            isFileCreated=True

        if(isFileCreated):
            if(filedata['upload_type']=="S3"):
                    awsFunctions.upload_file_to_s3(file_name=tempfilepath,bucket_name=filedata['bucket_name'],object_name=filedata['s3_path'])
            else:  
                destination_path = filedata['filepath']
                shutil.copyfile(tempfilepath, destination_path)


    def read_edi_file(self,file_path):
        with open(file_path, 'r') as f:
            edi_data = f.read()
            segments = edi_data.split('\n')
            for segment in segments:
                elements = segment.split('*')
                for element in elements:
                    print(element)
    
    def writeAvro(self,row,filepath,schema):
        with open(filepath, "wb") as avro_file:
            fastavro.writer(avro_file, schema, [row])
    
    def readAvro(self,avro_file,returnDict=False):
        with open(avro_file, 'rb') as f:
            avro_reader = fastavro.reader(f)
            records = [record for record in avro_reader]
        df = pd.DataFrame(records)
        if(returnDict):
            record_dict = {key: str(value) for key, value in df.to_dict().items()}
            return record_dict
        return record_dict
        
    


# edi_file_path = 'sample.edi'
# FileFunction().read_edi_file(edi_file_path)
    