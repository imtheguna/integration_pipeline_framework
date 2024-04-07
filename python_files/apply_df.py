import traceback
from audit_functions import *
import pandas as pd
import duckdb

class DQValidation:
    def __init__(self) -> None:
        pass
        self.screen,self.row_rule,self.profile_rule=[],[],[]
        self.cursor = ''
    def readMetadata(self,cursor,screen_id=0):
        self.cursor = cursor 
        cursor.execute(f"select * from screen where screen_id='{screen_id}'")
        screen = cursor.fetchall()
        if(len(screen)!=0):
            for i in range(len(screen)):
                self.screen.append(commonFunctions.create_record(screen[i],[desc[i] for desc in cursor.description]))
        cursor.execute(f"select * from dq_rules where screen_id='{screen_id}' and rule_type='filter' ")
        row_rule = cursor.fetchall()
        if(len(row_rule)!=0):
            for i in range(len(row_rule)):
                self.row_rule.append(commonFunctions.create_record(row_rule[i],[desc[0] for desc in cursor.description]))
        cursor.execute(f"select * from dq_rules where screen_id='{screen_id}' and rule_type='profile' ")
        profile_rule = cursor.fetchall()
        if(len(profile_rule)!=0):
            for i in range(len(profile_rule)):
                self.profile_rule.append(commonFunctions.create_record(profile_rule[i],[desc[0] for desc in cursor.description]))
    def validatingMetadata(self):
        if(len(self.screen)==0):
            logging(logtype=Log.info,msg='No DQ Screen, DQ Process Skipped.')
            return False
        if(len(self.screen)>1):
            logging(logtype=Log.info,msg='To Many Screens')
            return False
        else:
            isRowValid = True
            isProfileValid = True
            if(len(self.row_rule)==0):
                logging(logtype=Log.info,msg='No Filter Rules')
                isRowValidate = False
            if(len(self.profile_rule)==0):
                logging(logtype=Log.info,msg='No Profile Rules')
                isProfileValidate = False
            if(isProfileValid or isRowValid):
                return True
            return False
    
    def addRowErrorLog(self,screen_key,rule_ds,rule_id,error_record_count):
        query = f"""INSERT INTO public.dq_row_alert_log(
                    scr_run_key, rule_id, rule_ds, error_record_count, rec_ins_ts)
                    VALUES ({screen_key},{rule_id},'{rule_ds}', {error_record_count}, current_timestamp);"""
        self.cursor.execute(query)   

    def addProfileErrorLog(self,screen_key,rule_ds,rule_id,error_record_count):
        query = f"""INSERT INTO public.dq_profile_alert_log(
                    scr_run_key, rule_id, rule_ds, error_record_count, rec_ins_ts)
                    VALUES ({screen_key},{rule_id},'{rule_ds}', {error_record_count}, current_timestamp);"""
        self.cursor.execute(query)   

    def applyRule(self,data):
        scr_key = -1
        rowDQCount = 0
        profileDQCount = 0
        filtered_df = []
        try:
            logging(logtype=Log.info,msg='DQ Started')
            self.screen = self.screen[0]
            audit_query = f"""INSERT INTO public.dq_scr_run_log(
                                screen_id, start_time, status, rec_ins_ts)
                                VALUES ({self.screen['screen_id']}, current_timestamp, 'Started', current_timestamp)"""
            self.cursor.execute(audit_query)   
            self.cursor.execute(f"select max(scr_run_key) from public.dq_scr_run_log")
            scr_data = self.cursor.fetchone()
            scr_key = scr_data[0]
            
            error_file = temp_path+f"{self.screen['screen_name']}_{commonFunctions.getCurrentDateTime('%Y_%m_%d%H_%M_%S')}.csv"
            
            ## Validating Row Rules
            if(len(self.row_rule)>0):
                logging(logtype=Log.info,msg='Row Rules Started')
                for rule in self.row_rule:
                    df = duckdb.query_df(data,virtual_table_name=self.screen['screen_name'],sql_query=rule['rule_query']).to_df()
                    df.to_csv(error_file,mode='a', header=False, index=False)
                    self.addRowErrorLog(screen_key=scr_key,rule_ds=rule['rule_ds'],rule_id=rule['rule_id'],error_record_count=len(df))
                    rowDQCount = rowDQCount+len(df)
                if(rowDQCount!=0):
                    df =  pd.read_csv(error_file,header=None)
                    df.columns = data.columns
                    merged_df = pd.merge(data, df, how='inner')
                    filtered_df = data[~data.isin(merged_df.to_dict(orient='list')).all(axis=1)]
                    error_df = merged_df.drop_duplicates()
                    error_df.to_csv(error_file, header=True, index=False) 
                    rowDQCount = len(error_df)
                    logging(logtype=Log.info,msg=f"DQ Error File name - {error_file.split('/')[-1]}")
                    logging(logtype=Log.info,msg=f'DQ Error records from row rules {len(error_df)}')
                    rope = awsFunctions.upload_file_to_s3(error_file,bucket_name=self.screen['error_bucket']['bucket'],
                                                object_name=self.screen['error_bucket']['path']+error_file.split('/')[-1])
                    logging(logtype=Log.info,msg='File Uploading into S3')
                    logging(logtype=Log.info,msg='Error File Path '+rope)

            # Validating Profile Rules
            if(len(self.profile_rule)>0):
                logging(logtype=Log.info,msg='Profile Rules Started')
                for rule in self.profile_rule:
                    df = duckdb.query_df(data,virtual_table_name=self.screen['screen_name'],sql_query=rule['rule_query']).to_df() 
                    self.addProfileErrorLog(screen_key=scr_key,rule_ds=rule['rule_ds'],rule_id=rule['rule_id'],error_record_count=len(df))
                    profileDQCount = profileDQCount+len(df)
            
            update_query = f"""update public.dq_scr_run_log
                                set end_time=current_timestamp,status='Success'
                                where scr_run_key in ({scr_key})"""
                                
            self.cursor.execute(update_query)    
            logging(logtype=Log.info,msg='DQ Completed')
            if(rowDQCount):
                awsFunctions.sendMail(toMail=self.screen['email'],fromMail='GUNATWIN6@gmail.com',sub='Row DQ Alert',
                                  body=f"""Hi,\n \nWe got Row DQ records for flow {self.screen['screen_name']}
                                                \nCount- {rowDQCount} \nPath - {rope}""",AWS_REGION='us-east-1')
            if(profileDQCount):
                awsFunctions.sendMail(toMail=self.screen['email'],fromMail='GUNATWIN6@gmail.com',sub='Profile DQ Alert',
                                  body=f"""Hi,\n \nWe got Profile DQ records for flow {self.screen['screen_name']}
                                                \nCount - {profileDQCount}""",AWS_REGION='us-east-1')
            if(filtered_df is None):
                filtered_df = []
            return filtered_df
        except Exception as e:
            error = traceback.format_exc()
            print(error)
            logging(logtype=Log.error,msg=error)
            end_time = commonFunctions.getCurrentDateTime('%Y-%m-%d% %H:%M:%S')
            update_query = f"""update public.dq_scr_run_log
                                set end_time=current_timestamp,status='Failed',error_msg='{error}'
                                where scr_run_key in ({scr_key})"""
            self.cursor.execute(update_query)       






        


