#!/bin/bash
# 调用方式与原脚本完全一致
# sh step10_load_brand_query_app_score_zhibiao_table.sh "/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data" "hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
local_project_data_path=$1
hdfs_project_data_path=$2
day=`date -d -1days +%Y-%m-%d`

# Spark 配置：指定队列、内存、核心数，适配集群资源
spark3-sql \
--name "load_brand_data_to_orc_zstd_table" \
--queue root.biz.adnet \
--conf spark.executor.memory=8G \
--conf spark.driver.memory=4G \
--conf spark.executor.cores=4 \
--conf spark.sql.orc.compression.codec=zstd \
-e "
-- 1. 删除目标分区（原子操作，避免数据重复）
ALTER TABLE ads_strategy_adnet.da_brand_keywords_v2
DROP IF EXISTS PARTITION (day='${day}');

-- 2. 使用CSV数据源读取制表符分隔文件
CREATE TEMPORARY VIEW tmp_step9_data
USING csv
OPTIONS (
  path '${hdfs_project_data_path}/step9_get_search_index_tran_space_suggest_price',
  sep '\t',          -- 制表符分隔
  header 'false',    -- 无表头
  inferSchema 'false'-- 关闭自动类型推断，提升性能
);

-- 3. 核心修复：将 col1~col6 改为 _c0~_c5 匹配Spark默认列名
INSERT OVERWRITE TABLE ads_strategy_adnet.da_brand_keywords_v2
PARTITION (day='${day}')
SELECT
  cast(_c0 as string) as app_pkg,
  cast(_c1 as string) as query,
  cast(_c2 as int) as search_index,
  cast(_c3 as int) as potential,
  cast(_c4 as double) as suggested_bid,
  cast(_c5 as double) as score
FROM tmp_step9_data;
"
