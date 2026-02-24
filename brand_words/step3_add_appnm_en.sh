#!/bin/bash
#####################################
# @path:
# @Author:
# Date: 
# sh step3_add_appnm_en.sh "/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data" "hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
######################################
start_date=`date -d -1days +%Y-%m-%d`
end_date=`date -d -1days +%Y-%m-%d`

local_project_data_path=$1
hdfs_project_data_path=$2

hive -e "
set mapreduce.job.queuename = root.biz.adnet;
set mapred.reduce.tasks = 200;
set hive.mapred.mode=nonstrict;

CREATE TEMPORARY TABLE temp1_appid_nms(
app_id string,
raw_app_name string,
app_nm_pre string,
app_nm_py1 string,
app_nm_py2 string,
app_nm_py3 string,
app_nm_py4 string,
app_nm_abb1 string,
app_nm_abb2 string,
app_nm_abb3 string,
app_nm_py_abb1 string,
app_nm_py_abb2 string,
app_nm_py_abb3 string,
app_cn_py_m1 string,
app_cn_py_m2 string,
app_cn_py_m3 string,
app_cn_py_m4 string,
app_cn_py_m5 string,
app_cn_py_m6 string,
app_cn_py_abb1 string,
app_cn_py_abb2 string,
app_cn_py_abb3 string,
app_cn_py_abb4 string
)row format delimited
fields terminated by '\t'
STORED AS TEXTFILE;
load data inpath '${hdfs_project_data_path}/step1_get_app_nms' overwrite into table temp1_appid_nms;

CREATE TEMPORARY TABLE temp2_appid_appnm_en(
app_id string,
app_nm string,
app_nm_en string
)row format delimited
fields terminated by '\t'
STORED AS TEXTFILE;
load data local inpath '${local_project_data_path}/step2_app_nm_Cn2En' overwrite into table temp2_appid_appnm_en;

INSERT OVERWRITE DIRECTORY '${hdfs_project_data_path}/step3_add_appnm_en'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
select a.*, app_nm_en
from(
select *
from temp1_appid_nms
) a
left join
(
select app_id,app_nm,app_nm_en
from temp2_appid_appnm_en
) b
on a.app_id=b.app_id
"
