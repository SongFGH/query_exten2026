#!/bin/bash
#####################################
# @path:
# @Author:
# Date: 
# sh step9_get_search_index_tran_space_suggest_price.sh "/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data" "hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
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
app_id string,
dl_ratio double,
cnt int,
normalized_dl_ratio double,
normalized_cnt double,
score double
)row format delimited
fields terminated by '\t'
STORED AS TEXTFILE;
load data inpath '${hdfs_project_data_path}/step8_get_score' overwrite into table temp1_query_appid;

with t0 as (
select distinct
    e.query,
    e.app_id,
    score,
    search_index,
    trans_space,
    nvl(bid, 1.5) as bid
from
    (
        select distinct
            c.query,
            c.app_id,
            score,
            search_index,
            nvl(trans_space, 0) as trans_space
        from
            (
                select distinct
                    a.query,
                    app_id,
                    score,
                    nvl(search_index, 0) as search_index
                from
                    (
                        select distinct
                            query,
                            app_id,
                            score
                        from temp1_query_appid
                        where 1 > 0
                    ) a
                    left join (
                        select distinct
                            query,
                            search_index
                        from
                            es2_ads_dev.da_accuracy_word_first_location_search_index
                        where 1 > 0
                        -- and day = '2026-01-20'
                        and day >= '${start_date}' and day <= '${end_date}'
                    ) b on a.query = b.query
            ) c
            left join (
                select distinct
                    query,
                    app_id,
                    trans_space
                from
                    es2_ads_dev.da_accuracy_word_first_location_trans_space
                where 1 > 0
                -- and day = '2026-01-20'
                and day >= '${start_date}' and day <= '${end_date}'
            ) d on c.query = d.query
            and c.app_id = d.app_id
    ) e
    left join (
        select distinct
            query,
            app_id,
            bid
        from
            es2_ads_dev.da_accuracy_word_first_location_sugget_price
        where 1 > 0
        -- and day = '2026-01-20'
        and day >= '${start_date}' and day <= '${end_date}'
    ) f on e.query = f.query
    and e.app_id = f.app_id
)
,t1 as (
    select DISTINCT id,app_package
    from bi_appstore_dev.dm_appstore_appinfo
    where 1 > 0
    -- and day = '2026-01-20'
    and day >= '${start_date}' and day<='${end_date}'
    and app_status <> -1 and app_status_safe in (0,13)
)
INSERT OVERWRITE DIRECTORY '${hdfs_project_data_path}/step9_get_search_index_tran_space_suggest_price'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
select app_package,
    query,
    search_index,
    trans_space,
    nvl(bid, 1.5) as bid,
    score
from t0
inner join t1 on t0.app_id = t1.id
"
