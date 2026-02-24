#!/bin/bash
#####################################
# @path:
# @Author:
# Date: 
# sh step8_get_score.sh "/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data" "hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
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
cnt int
)row format delimited
fields terminated by '\t'
STORED AS TEXTFILE;
load data inpath '${hdfs_project_data_path}/step7_query_appid_filter' overwrite into table temp1_query_appid;

-- 步骤1：先通过窗口函数获取dl_ratio和cnt的全局最大/最小值
WITH temp_normalize_params AS (
    SELECT
        -- 获取dl_ratio的全局最大、最小值
        MAX(dl_ratio) OVER () AS dl_ratio_max,
        MIN(dl_ratio) OVER () AS dl_ratio_min,
        -- 获取cnt的全局最大、最小值
        MAX(cnt) OVER () AS cnt_max,
        MIN(cnt) OVER () AS cnt_min,
        -- 保留原表所有字段
        query,
        app_id,
        dl_ratio,
        cnt
    FROM temp1_query_appid
),

-- 步骤2：套用0-1归一化公式计算两个字段的归一化值，再求和得到score
temp_calc_score AS (
    SELECT
        query,
        app_id,
        dl_ratio,
        cnt,
        -- 计算dl_ratio的0-1归一化值（处理分母为0的边界情况）
        CASE
            WHEN dl_ratio_max = dl_ratio_min THEN 0.0
            ELSE (dl_ratio - dl_ratio_min) / (dl_ratio_max - dl_ratio_min)
        END AS normalized_dl_ratio,
        -- 计算cnt的0-1归一化值（处理分母为0的边界情况）
        CASE
            WHEN cnt_max = cnt_min THEN 0.0
            ELSE (cnt - cnt_min) / (cnt_max - cnt_min)
        END AS normalized_cnt
    FROM temp_normalize_params
)
INSERT OVERWRITE DIRECTORY '${hdfs_project_data_path}/step8_get_score'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
-- 步骤3：输出最终结果（归一化值 + score）
SELECT
    query,
    app_id,
    dl_ratio,
    cnt,
    normalized_dl_ratio,
    normalized_cnt,
    -- 归一化值求和得到score
    (normalized_dl_ratio + normalized_cnt) AS score
FROM temp_calc_score;
"
