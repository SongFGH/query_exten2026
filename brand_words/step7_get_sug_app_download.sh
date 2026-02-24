#!/bin/bash
# 功能：提取前1天的keyword&cnt数据，保存到指定HDFS路径
# 用法：sh step7_get_sug_app_download.sh [本地路径] [HDFS路径]
# 示例：sh step7_get_sug_app_download.sh "/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data" "hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"

# 1. 配置日期参数：自动获取前1天的日期（start_date和end_date均为前1天）
start_date=`date -d -1days +%Y-%m-%d`
end_date=`date -d -1days +%Y-%m-%d`

# 2. 接收脚本传入的两个参数：本地路径、HDFS路径
local_project_data_path=$1
hdfs_project_data_path=$2

# 3. 校验HDFS路径参数是否传入（核心：保存数据到HDFS，该参数不可为空）
if [ -z "${hdfs_project_data_path}" ]; then
    echo "===== 错误：请传入HDFS路径作为第二个参数 ====="
    exit 1
fi

# 4. 定义数据提取并保存到HDFS的函数
function obtain_data()
{
    spark3-sql \
        --driver-memory 12G \
        --executor-cores 8 \
        --executor-memory 20G \
        --conf spark.dynamicAllocation.maxExecutors=200 \
        --conf spark.dynamicAllocation.enabled=true \
        --queue root.biz.prd \
        -e "
            -- 关键2：关闭所有压缩相关配置，确保明文不被压缩
            SET hive.cli.print.header=false;
            SET hive.exec.compress.output=false;
            SET mapred.output.compress=false;
            SET mapreduce.output.fileoutputformat.compress=false;
            SET hive.exec.dynamic.partition.mode=nonstrict;
            
            -- 核心修正：SQL 语法格式正确，STORED AS TEXTFILE 紧跟 DIRECTORY 配置
            INSERT OVERWRITE DIRECTORY '${hdfs_project_data_path}/step7_get_sug_app_download'
            ROW FORMAT DELIMITED 
            FIELDS TERMINATED BY '\t'
            LINES TERMINATED BY '\n'
            STORED AS TEXTFILE  -- 必须紧跟在 ROW FORMAT 之后，与 DIRECTORY 为同一语句

            SELECT search_word, appid, dl_ratio 
            FROM comsearch.da_appsearch_sug_app_download_ratio_ds 
            where 1 > 0
            -- and day = '2026-01-20'
            and day >= '${start_date}' and day<='${end_date}'
        
        "
}

# 5. 执行数据提取函数
echo "===== 开始提取数据，日期范围：${start_date} 至 ${end_date} ====="
echo "===== 数据将保存到HDFS路径：${hdfs_project_data_path}/step7_get_sug_app_download ====="
obtain_data

# 6. 验证HDFS路径是否生成数据（可选，增加脚本健壮性）
hdfs dfs -test -d "${hdfs_project_data_path}/step7_get_sug_app_download"
if [ $? -eq 0 ]; then
    echo "===== 数据保存成功，HDFS路径存在 ====="
else
    echo "===== 警告：HDFS目标路径不存在，数据保存可能失败 ====="
fi
