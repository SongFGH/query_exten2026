#!/bin/bash
# sh step1_get_app_nms.sh "/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data" "hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"

local_project_data_path=$1
hdfs_project_data_path=$2

function step1_get_app_nms()
{
    input_path=${hdfs_project_data_path}/step0_get_nature_comm_app_info
    output_path=${hdfs_project_data_path}/step1_get_app_nms
    set +e
    hadoop fs -rm -r ${output_path}.tmp
    set -e
    hadoop jar hadoop-streaming*.jar \
        -D mapreduce.job.queuename=root.biz.adnet \
        -D stream.non.zero.exit.is.failure=false \
        -D mapreduce.map.memory.mb=4096 \
        -D mapred.reduce.tasks=100 \
        -input ${input_path}/* \
        -output ${output_path}.tmp \
        -mapper "python/miniforge3/bin/python step1_mapper.py" \
        -reducer "python/miniforge3/bin/python step1_reducer.py" \
        -file ./step1_mapper.py \
        -file ./step1_reducer.py \
        -cacheArchive hdfs://bj04-region06/region06/17200/app/develop/longcanwu/wlcenvs.tar.gz#python

    if [[ $? -ne 0 ]];then
        echo "failed"
        return 1
    fi
    set +e
    hadoop fs -rmr ${output_path}
    set -e
    hadoop fs -mv ${output_path}.tmp ${output_path}
}
step1_get_app_nms
