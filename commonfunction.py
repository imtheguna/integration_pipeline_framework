from collections import namedtuple
from datetime import *
import random
import string 
import zlib
import os
import shutil
import hashlib
import pandas as pd

class CommonFunctions:
    def __init__(self) -> None:
        pass

    ## adding header for df
    def create_record(self,obj, fields):
        mappings = dict(zip(fields, obj))
        return mappings
    
    ## checking the bool value
    def getBool(self,value):
        return str(value).lower() in ['true', '1', 't', 'y', 'yes',]
    
    ## validting metadata
    def validateMeta(self,metadata):
        if(metadata['loadtype'].lower() not in ['filetotable','tabletofile','tabletotable','debatch','filetofile']):
            return 'Load Type not supported '+metadata['loadtype']
        if(metadata['source'].lower() not in ['table','file'] or metadata['target'].lower() not in ['table','file','debatch']):
            return f'Source or Target Type not supported'
        if (metadata['source'].lower() =='file' and metadata['src_file_metadata'] is None):
            return 'Source file info missing'
        if (metadata['target'].lower() =='file' and metadata['tgt_file_metadata'] is None):
            return 'Target file info missing'
        if (metadata['target'].lower() =='table' and metadata['tgt_table_info'] is None):
            return 'Target table info missing'
        if (metadata['source'].lower() =='table' and metadata['src_table_info'] is None):
            return 'Source table info missing'
        if (metadata['source'].lower() =='table' and metadata['src_database_meta_data'] is None):
            return 'Source database info missing'
        if (metadata['target'].lower() =='table' and metadata['tgt_database_meta_data'] is None):
            return 'Target database info missing'
        return True
    
    ## reading the current date and time
    def getCurrentDateTime(self,format):
        return datetime.now().strftime(format)
    
    ## CRC
    def calculate_crc(self,row):
        # Convert the row to a string
        row_str = ','.join(map(str, row))
        
        # Calculate CRC value using zlib.crc32()
        crc_value = zlib.crc32(row_str.encode())
        
        return crc_value

    ## adding columns from table info metadata from drvr table
    def addColumns(self,columns,df,current_user,current_file):
        columns_added = []
        isCRC = False
        for i,j in columns.items():
            if(j=='crc'):
                isCRC = True
                pass
            columns_added.append(i)
            if(j=='current_timestamp'):
                df[i] = self.getCurrentDateTime('%Y-%m-%d %H:%M:%S')
            elif(j=='current_date'):
                df[i] = self.getCurrentDateTime('%Y-%m-%d')
            elif(j=='current_user'):
                df[i]=current_user
            elif(j=='source_filename'):
                df[i]=current_file
            elif(j!='crc'):
                df[i]=j
        if(isCRC):
            columns_added.remove('crc')
            modified_df = df.drop(columns=columns_added)
            modified_df['crc'] = modified_df.apply(self.calculate_crc, axis=1)
            df = pd.concat([df, modified_df['crc']], axis=1)
        return df

    def cleanup(self,folder_path):
        if(folder_path[-1]=='/'):
            folder_path = folder_path[:-1]
        # Delete the folder if it exists
        if os.path.exists(folder_path):
            # Remove the folder and its contents
            shutil.rmtree(folder_path)
            print(f"Folder '{folder_path}' deleted.")

        # Recreate the folder
        os.makedirs(folder_path)
        print(f"Folder '{folder_path}' recreated.")
    
    def generate_random_string(self,length):
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

    def hash_string(self,input_string):

        sha256_hash = hashlib.sha256()
        sha256_hash.update(input_string.encode())
        hashed_string = sha256_hash.hexdigest()
        
        return hashed_string
    
    