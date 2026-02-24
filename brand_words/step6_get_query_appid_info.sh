#!/bin/bash
#####################################
# @path:
# @Author:
# Date: 
# sh step6_get_query_appid_info.sh "/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data" "hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
######################################
start_date=`date -d -1days +%Y-%m-%d`
end_date=`date -d -1days +%Y-%m-%d`

local_project_data_path=$1
hdfs_project_data_path=$2

hive -e "
set mapreduce.job.queuename = root.biz.adnet;
set mapred.reduce.tasks = 200;
set hive.mapred.mode=nonstrict;

CREATE TEMPORARY TABLE temp1_appid(
app_id string
)row format delimited
fields terminated by '\t'
STORED AS TEXTFILE;
load data inpath '${hdfs_project_data_path}/step5_get_query_appid_input' overwrite into table temp1_appid;

CREATE TEMPORARY TABLE temp2_appid_query(
app_id string,
query string
)row format delimited
fields terminated by '\t'
STORED AS TEXTFILE;
load data local inpath '${local_project_data_path}/step4_gen_appnm_accword' overwrite into table temp2_appid_query;

INSERT OVERWRITE DIRECTORY '${hdfs_project_data_path}/step6_get_query_appid_info'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
select query,b.app_id
from(
select app_id
from temp1_appid
) a
inner join
(
select app_id,query
from temp2_appid_query
) b
on a.app_id=b.app_id
"
