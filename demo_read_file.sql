INSERT INTO public.etl_load_drvr(
	loadtype, source, target, name,
	tgt_database_meta_data, tgt_table_info, 
	src_file_metadata, rec_ins_ts, dq_screen_id)
	VALUES ('filetotable', 'file', 'table','demo_read_file', cast('{
				 "DBType":"PostgresSQL",
				 "hostname":"localhost",
				 "port":5432,
				 "database":"postgres",
				 "user":"postgres",
				 "password_type":"password",
				 "password":"*********",
				 "schema":"public"
				 }' as json), cast('{
								   "tablename":"test",
								    "schema":"public",
				 					"encoding":"utf-8",
				 					"loadtype":"copy",
				 					"addcolumns":{
				 "rec_ins_ts":"current_timestamp",
				 "rec_ins_user_name":"current_user",
				 "src_file_nm":"source_filename",
				 "crc":"crc"
				 }}'as json), cast('{
				 "filename":"test_*.txt",
				 "header":"yes",
				 "customHeader":"",
				 "filetype":"text",
				 "delimiter":",",
				 "filepath":"",
				 "source":"S3",
				 "bucket_name":"task-12",
				 "s3_path":"in/",
				 "archive_path":"archive/"
				 }' as json), current_timestamp, 1);

/*  This is for reading the files from Local

{
				 "filename":"test_dq_*.txt",
				 "header":true,
				 "customHeader":"",
				 "source":"local",
				 "filetype":"text",
				 "delimiter":",",
				  "filepath":"/Users/guna/Study/python/load/",
				 "archive_path":"/Users/guna/Study/python/load/archive/"
				 }
*/