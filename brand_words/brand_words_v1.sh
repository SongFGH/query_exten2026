#!/bin/bash
#####################################
# @file: 
# @author: 
# @Date: 
#####################################

#ip:10.200.87.13
#任务：垂搜-首位广告推词-品牌词生成，brand_words #!!!,可以改动
#脚本路径：cd /data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words #!!!,可以改动
#逻辑：
#输入：
#输出：
#每一行的格式：
#部分数据如下：

#整体逻辑：
# 1.输入app信息表获取appid和app-name到${hdfs_project_data_path}/step0_get_nature_comm_app_info
# 2.基于step0_get_nature_comm_app_info生成衍生词到${hdfs_project_data_path}/step1_get_app_nms
# 3.从step1_get_app_nms提取appid和app简称词生成英文到${local_project_data_path}/step2_app_nm_Cn2En
# 4.合并英文和其他衍生词，然后分割成appid-query到${local_project_data_path}/step4_gen_appnm_accword
# 5.输入曝光表中的去重的appid的集合，然后补充appid对应的衍生词到${hdfs_project_data_path}/step6_get_query_appid_info
# 6.获取query的月度搜索量、下载集中度数据，对step6_get_query_appid_info中的query-appid进行过滤，然后算出score输出到${hdfs_project_data_path}/step8_get_score
# 7.针对step8_get_score补充搜索指数等信息，最后上传到ads_strategy_adnet.da_brand_keywords_v2
#输出：
# ads_strategy_adnet.da_brand_keywords_v2

# 1. 时间参数处理：获取前一天的日期（格式：年-月-日 时:分:秒），用于后续数据版本/目录命名
pre_data=`date -d -1days "+%Y-%m-%d %H:%M:%S"`
# 将前一天的日期转换为时间戳（秒级），作为唯一标识避免目录冲突
timeStamp=`date -d "$pre_data" +%s`

# 2. 路径配置：定义脚本运行、数据存储、HDFS存储、Python环境等核心路径
#项目名称
project_name="brand_words" #!!!,可以改动
#项目基础路径
local_base_path="/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120" #!!!,可以改动
# 项目根路径
local_project_path="${local_base_path}/${project_name}"
# 本地数据存储基础路径
local_project_data_path="${local_project_path}/data"
#HDFS基础路径
hdfs_base_path="hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120" #!!!,可以改动
# HDFS项目基础路径
hdfs_project_path="${hdfs_base_path}/${project_name}"
# HDFS项目数据路径（存储中间结果和最终结果）
hdfs_project_data_path="${hdfs_project_path}/data"
# 公共数据存储路径（存放跨步骤复用的基础数据）
common_data_path="${local_base_path}/common_data"
# 定义HDFS通用数据路径（分布式文件系统的通用数据仓，供所有子项目读取）
hdfs_common_data_path="${hdfs_base_path}/common_data"

# 3. 环境与目录清理：容错处理+清理历史数据
# set +e：关闭脚本的严格错误检测（即使某条命令失败，脚本仍继续执行），避免清理目录失败导致脚本中断
set +e
# 删除HDFS上以时间戳命名的历史目录（避免重复）
hdfs dfs -rm -r ${hdfs_project_path}/${timeStamp}
# 清空HDFS基础路径下的所有文件/目录（清理上次运行的中间结果）
hdfs dfs -rm -r ${hdfs_project_data_path}/*
set -e

# 进入本地数据基础路径
cd ${local_project_data_path}
set +e
# 开启Shell的扩展通配符功能（支持!(xxx)语法：排除指定文件）
shopt -s extglob
# 删除本地数据目录下的所有文件（清空历史本地数据）
rm !(step2_app_nm_Cn2En_cache)
#step2_app_nm_Cn2En_cache,!!!,这个数据是历史任务生成的，新任务可以复制这个数据
# 关闭扩展通配符功能（恢复默认）
shopt -u extglob
# set -e：恢复严格错误检测（后续命令出错则脚本终止）
set -e

# 回到项目根路径
cd ${local_project_path}

#!!!,如果需要执行python脚本，需要执行如下命令
# 激活指定Conda环境
CONDA_ROOT="/home/11179767/miniforge3" #!!!,可以更改
TARGET_ENV="tuiciv1"
# 初始化+激活
source "${CONDA_ROOT}/etc/profile.d/conda.sh"
conda activate "${TARGET_ENV}"
# 执行测试命令
python --version

# local_project_data_path = "/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
# hdfs_project_data_path = "hdfs://bj04-region06/region06/17200/app/develop/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
sh step0_get_nature_comm_app_info.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：获取appId的基础信息
#输入：bi_appstore_dev.dm_appstore_appinfo,!!!,内容库应用信息主表,来自任务：dm_appstore_appinfo,需要手动依赖
#输出：${hdfs_project_data_path}/step0_get_nature_comm_app_info
#每一行的格式：appid,app_cn_name

sh step1_get_app_nms.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：基于step0_get_nature_comm_app_info新增多列：中文名称||应用中文简称||拼音名称词||中文简写词||拼音简写词||中文和拼音混合词||中文和拼音缩写词
#输入：${hdfs_project_data_path}/step0_get_nature_comm_app_info
#输入：hadoop-streaming*.jar,step1_mapper.py,step1_reducer.py
#输入：wlcenvs.tar.gz，家目录的miniforge3打包而成
#输出：${hdfs_project_data_path}/step1_get_app_nms
#每一行的格式：appid,
# raw_app_name,app_nm_pre,
# app_nm_py1,app_nm_py2,app_nm_py3,app_nm_py4,
# app_nm_abb1,app_nm_abb2,app_nm_abb3,
# app_nm_py_abb1,app_nm_py_abb2,app_nm_py_abb3,
#app_cn_py_m1,app_cn_py_m2,app_cn_py_m3,app_cn_py_m4,app_cn_py_m5,app_cn_py_m6,
#app_cn_py_abb1,app_cn_py_abb2,app_cn_py_abb3,app_cn_py_abb4

# exec 3>${文件路径}：打开文件描述符3，指向目标文件（用于重定向输出，比直接>更稳定）
exec 3>${local_project_data_path}/step1_2_appid_nm
hdfs dfs -cat ${hdfs_project_data_path}/step1_get_app_nms/* | awk -F '\t' '{print $1"\t"$3}' >&3
#逻辑：读取HDFS上step1的输出文件，按制表符分割，提取第1列（APPID）和第3列（APP简称），输出到文件描述符3
#输入：${hdfs_project_data_path}/step1_get_app_nms
#输出：{local_project_data_path}/step1_2_appid_nm
#每一行的格式：app_id,app_nm_pre

python step2_app_nm_Cn2En.py #!!!,文件里面有路径，需要更改的话要更改
#逻辑：将APP中文名称转换为英文
#输入：${local_project_data_path}/step1_2_appid_nm, ${local_project_data_path}/step2_app_nm_Cn2En_cache,!!!,这个缓存文件可以复用到新任务
#输出：${local_project_data_path}/step2_app_nm_Cn2En, ${local_project_data_path}/step2_app_nm_Cn2En_cache
#每一行的格式：app_id,app_nm_pre,app_nm_english;app_id,app_nm_pre,app_nm_english

sh step3_add_appnm_en.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：针对step1_get_app_nms添加一列英文名称
#输入：${hdfs_project_data_path}/step1_get_app_nms, ${local_project_data_path}/step2_app_nm_Cn2En
#输出：${hdfs_project_data_path}/step3_add_appnm_en
#每一行的格式：appid,中文衍生词(很多个),app_nm_english

# 打开文件描述符3，指向目标文件
exec 3>${local_project_data_path}/step3_2_add_appnm_en
# 读取HDFS上step3的压缩输出文件，按制表符分割，输出完整行到本地文件
hdfs dfs -cat ${hdfs_project_data_path}/step3_add_appnm_en/* | awk -F '\t' '{print $0}' >&3

python step4_gen_appnm_accword.py #!!!,文件里面有路径，需要更改的话要更改
#逻辑：为每个appId生成「多维度匹配关键词」，需要去重复，最终输出「appId \t 匹配关键词」的标准化数据
#输入：${local_project_data_path}/step3_2_add_appnm_en
#输出：${local_project_data_path}/step4_gen_appnm_accword
#每一行的格式： app_id \t 衍生词
#部分数据如下：
# 1578375 运车管家-发车人版软件
# 1578375 运车管家
# 1578375 yuncheguanjia

sh step5_get_query_appid_input.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：获取query-appId输入中的appid的去重以后的集合
#输入：appstore_search.da_appstore_search_query_conversion_di,!!!,商店搜索搜索词转换表,需要手动依赖,这是初始的query-appid的集合
#输出：${hdfs_project_data_path}/step5_get_query_appid_input
#每一行的格式：appid

sh step6_get_query_appid_info.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：针对step5_get_query_appid_input补充appId的query
#输入：${hdfs_project_data_path}/step5_get_query_appid_input,${local_project_data_path}/step4_gen_appnm_accword
#输出：${hdfs_project_data_path}/step6_get_query_appid_info
#每一行的格式：query,appid

# 数据备份,方便debug，可以注释掉
hdfs dfs -cp ${hdfs_project_data_path}/step6_get_query_appid_info ${hdfs_project_data_path}/step6_get_query_appid_info_beifen

sh step7_get_query_cnt.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：获取query-cnt的数据
#输入：ads_strategy_adnet.dw_response_analysis_query_last_month_cnt_d,!!!,query月度搜索量，需要手动依赖
#输出：${hdfs_project_data_path}/step7_get_query_cnt
#每一行的格式：keyword, cnt

# 数据备份,方便debug，可以注释掉
hdfs dfs -cp ${hdfs_project_data_path}/step7_get_query_cnt ${hdfs_project_data_path}/step7_get_query_cnt_beifen


sh step7_get_sug_app_download.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：获取query-app的下载集中度数据
#输入：comsearch.da_appsearch_sug_app_download_ratio_ds,!!!,联想页混排_APP下载集中度表，需要手动依赖
#输出：${hdfs_project_data_path}/step7_get_sug_app_download
#每一行的格式：search_word, appid, dl_ratio

# 数据备份,方便debug，可以注释掉
hdfs dfs -cp ${hdfs_project_data_path}/step7_get_sug_app_download ${hdfs_project_data_path}/step7_get_sug_app_download_beifen

sh step7_get_search_app_download.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：获取query-app的下载集中度数据
#输入：comsearch.da_appsearch_search_app_download_ratio_ds,!!!,结果页_APP下载集中度表，需要手动依赖
#输出：${hdfs_project_data_path}/step7_get_search_app_download
#每一行的格式：search_word, appid, dl_ratio

# 数据备份,方便debug，可以注释掉
hdfs dfs -cp ${hdfs_project_data_path}/step7_get_search_app_download ${hdfs_project_data_path}/step7_get_search_app_download_beifen

sh step7_query_appid_filter.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：针对step6_get_query_appid_info按照条件（该词下的app下载集中度>50%且月度词搜索量>=3w）筛选
#输入：${hdfs_project_data_path}/step6_get_query_appid_info
#输入：${hdfs_project_data_path}/step7_get_query_cnt
#输入：${hdfs_project_data_path}/step7_get_sug_app_download,${hdfs_project_data_path}/step7_get_search_app_download
#输出：${hdfs_project_data_path}/step7_query_appid_filter
#每一行的格式：query,appid,dl_ratio,cnt

# 数据备份,方便debug，可以注释掉
hdfs dfs -cp ${hdfs_project_data_path}/step7_query_appid_filter ${hdfs_project_data_path}/step7_query_appid_filter_beifen

sh step8_get_score.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：针对step7_query_appid_filter，下载集中度和月度词搜索量倒排进行排序，按照0-1归一化1:1算出来score
#输入：${hdfs_project_data_path}/step7_query_appid_filter
#输出：${hdfs_project_data_path}/step8_get_score
#每一行的格式：query,appid,dl_ratio,cnt,normalized_dl_ratio,normalized_cnt,score

# 数据备份,方便debug，可以注释掉
hdfs dfs -cp ${hdfs_project_data_path}/step8_get_score ${hdfs_project_data_path}/step8_get_score_beifen

sh step9_get_search_index_tran_space_suggest_price.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：针对step8_get_score补充新的3列,以及appid的基本信息（app_package）
#输入：${hdfs_project_data_path}/step8_get_score
#输入：bi_appstore_dev.dm_appstore_appinfo,!!!,内容库应用信息主表,来自任务：dm_appstore_appinfo,需要手动依赖
#输入：es2_ads_dev.da_accuracy_word_first_location_search_index,!!!,来自任务：垂搜-营销平台-query推荐搜索指数,需要手动依赖
#输入：es2_ads_dev.da_accuracy_word_first_location_trans_space,!!!,来自任务：垂搜营销平台query潜力空间,需要手动依赖
#输入：es2_ads_dev.da_accuracy_word_first_location_sugget_price,!!!,来自任务：垂搜-营销平台-query推荐建议出价,需要手动依赖
#输出：${hdfs_project_data_path}/step9_get_search_index_tran_space_suggest_price
#每一行的格式：app_pkg,query,search_index,potential,suggested_bid,score

# 数据备份,方便debug，可以注释掉
hdfs dfs -cp ${hdfs_project_data_path}/step9_get_search_index_tran_space_suggest_price ${hdfs_project_data_path}/step9_get_search_index_tran_space_suggest_price_beifen

sh step10_load_brand_query_app_score_zhibiao_table.sh "${local_project_data_path}"  "${hdfs_project_data_path}"
#逻辑：加载指标数据到数据表
#输入：${hdfs_project_data_path}/step9_get_search_index_tran_space_suggest_price
#输出：ads_strategy_adnet.da_brand_keywords_v2,!!!,该任务的输出
#每一行的格式：app_pkg,query,search_index,potential,suggested_bid,score,day


# HDFS数据归档：备份本次运行的所有结果
# 将HDFS基础路径的所有数据复制到以时间戳命名的归档目录（便于追溯/回滚）
hdfs dfs -cp ${hdfs_project_data_path} ${hdfs_project_path}/${timeStamp}
