#!/bin/bash
#####################################
# @path:
# @Author:
# Date: 
# sh step7_query_appid_filter.sh "/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data" "hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
######################################
start_date=`date -d -1days +%Y-%m-%d`
end_date=`date -d -1days +%Y-%m-%d`

local_project_data_path=$1
hdfs_project_data_path=$2

hive -e "
set mapreduce.job.queuename = root.biz.adnet;
set mapred.reduce.tasks = 200;
set hive.mapred.mode=nonstrict;

CREATE TEMPORARY TABLE temp1_query_appid(
query string,
app_id string
)row format delimited
fields terminated by '\t'
STORED AS TEXTFILE;
load data inpath '${hdfs_project_data_path}/step6_get_query_appid_info' overwrite into table temp1_query_appid;

CREATE TEMPORARY TABLE temp1_query_cnt(
keyword string,
cnt string
)row format delimited
fields terminated by '\t'
STORED AS TEXTFILE;
load data inpath '${hdfs_project_data_path}/step7_get_query_cnt' overwrite into table temp1_query_cnt;

CREATE TEMPORARY TABLE temp1_sug_app_download(
search_word string,
appid string,
dl_ratio string
)row format delimited
fields terminated by '\t'
STORED AS TEXTFILE;
load data inpath '${hdfs_project_data_path}/step7_get_sug_app_download' overwrite into table temp1_sug_app_download;

CREATE TEMPORARY TABLE temp1_search_app_download(
search_word string,
appid string,
dl_ratio string
)row format delimited
fields terminated by '\t'
STORED AS TEXTFILE;
load data inpath '${hdfs_project_data_path}/step7_get_search_app_download' overwrite into table temp1_search_app_download;


INSERT OVERWRITE DIRECTORY '${hdfs_project_data_path}/step7_query_appid_filter'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
-- 直接关联：临时表 + 内联子查询（替代WITH子句）
select 
  t3.query, 
  t3.app_id, 
  t1.dl_ratio, 
  t2.cnt
from 
  -- t3：直接引用临时表（替代WITH t3）
  temp1_query_appid t3
  -- 内联t1：替代WITH t1（先合并sug/search数据，再聚合max(dl_ratio)）
  inner join (
    select 
      search_word, 
      appid, 
      max(dl_ratio) as dl_ratio
    from (
      -- 内部子查询：替代WITH t0（合并sug/search数据）
      select * from temp1_sug_app_download
      union all
      select * from temp1_search_app_download
    ) t0
    where dl_ratio > 0.5  -- 过滤条件保留
    group by search_word, appid
  ) t1 on t3.query = t1.search_word and t3.app_id = t1.appid
  -- t2：直接引用临时表（替代WITH t2）
  inner join temp1_query_cnt t2 on t3.query = t2.keyword;
"
