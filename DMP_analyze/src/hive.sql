create table dmp_analyze(key string,allurl int,haskeyurl int) row format delimited fields terminated by ',' stored as textfile;
create table dmp_user_ac_bytype(key string,type int,ac int) row format delimited fields terminated by ',' stored as textfile;

LOAD DATA INPATH '/user/hadoop/data-analyze/hastitle/part-r-00000' OVERWRITE INTO TABLE dmp_analyze;

LOAD DATA INPATH '/user/hadoop/data-analyze/usercount/part-r-00000' OVERWRITE INTO TABLE dmp_user_ac_bytype;

CREATE EXTERNAL TABLE dmp_classify(uid string,ac int,dspCluster int)STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,uc:AC,uc:M")  TBLPROPERTIES ("hbase.table.name" = "dmp_user_classify");