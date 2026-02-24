#!/bin/bash
#####################################
# @path:
# @Author:
# Date: 
# sh step5_get_query_appid_input.sh "/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data" "hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
######################################
start_date=`date -d -1days +%Y-%m-%d`
end_date=`date -d -1days +%Y-%m-%d`

local_project_data_path=$1
hdfs_project_data_path=$2

hive -e "
set mapreduce.job.queuename = root.biz.adnet;
set mapred.reduce.tasks = 200;
set hive.mapred.mode=nonstrict;

INSERT OVERWRITE DIRECTORY '${hdfs_project_data_path}/step5_get_query_appid_input'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
select appid
from appstore_search.da_appstore_search_query_conversion_di
where 1 > 0
--and day = '2026-01-20'
and day >= '${start_date}' and day<='${end_date}'
and app_exposure_num > 0
group by appid
"
