
from commonfunction import CommonFunctions

commonFunctions = CommonFunctions()
log_path = './log/process_'+commonFunctions.getCurrentDateTime('%Y_%m_%d%H_%M_%S')+'.log'
temp_path = './temp/'
chunk_size = 1000
avro_temp_path = './avro/temp/'
