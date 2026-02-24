#coding:utf-8
import os

# 定义路径配置
base_path="/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
in_path = base_path+"/step3_2_add_appnm_en"
out_path = base_path+"/step4_gen_appnm_accword"

# 校验输入文件是否存在
if not os.path.exists(in_path):
    print(f"【错误】输入文件不存在：{in_path}")
    exit(1)

# 清理旧输出文件（若存在）
if os.path.exists(out_path):
    os.remove(out_path)

# 核心去重缓存：app_id -> 已收集的精准词列表
appid_word_dict = {}

# 逐行读取输入、处理数据、写入输出（with语句自动管理文件句柄）
with open(in_path, "r", encoding="utf-8") as infile, open(out_path, "w", encoding="utf-8") as out_file:
    for line in infile:
        line = line.strip()
        if not line:
            continue
        
        split_info = line.split("\t")
        # 新字段列表共21个字段，校验字段数量避免解包报错
        if len(split_info) != 24:
            print(f"【警告】字段数量错误（需24个，实际{len(split_info)}个），跳过该行：{line}")
            continue
        
        # 字段解包（新的字段列表）
        try:
            [app_id,
            raw_app_name,
            app_nm_pre,
            app_nm_py1, app_nm_py2, app_nm_py3, app_nm_py4,
            app_nm_abb1, app_nm_abb2, app_nm_abb3,
            app_nm_py_abb1, app_nm_py_abb2, app_nm_py_abb3,
            app_cn_py_m1, app_cn_py_m2, app_cn_py_m3, app_cn_py_m4, app_cn_py_m5, app_cn_py_m6,
            app_cn_py_abb1, app_cn_py_abb2, app_cn_py_abb3, app_cn_py_abb4,
            app_nm_english] = split_info
        except ValueError as e:
            print(f"【警告】字段解包失败，跳过该行：{line}，错误：{e}")
            continue
        
        # 1. 初始化当前app_id的缓存列表（若首次处理）
        if app_id not in appid_word_dict:
            appid_word_dict[app_id] = []
        
        # 2. 收集所有需要的精准词（整合所有相关字段，形成列表）
        all_acc_words = [
            # 核心名称字段
            raw_app_name, app_nm_pre, app_nm_english,
            # 拼音全拼字段
            app_nm_py1, app_nm_py2, app_nm_py3, app_nm_py4,
            # 中文简写字段
            app_nm_abb1, app_nm_abb2, app_nm_abb3,
            # 拼音简写字段
            app_nm_py_abb1, app_nm_py_abb2, app_nm_py_abb3,
            # 中拼混合字段
            app_cn_py_m1, app_cn_py_m2, app_cn_py_m3, app_cn_py_m4, app_cn_py_m5, app_cn_py_m6,
            # 中拼缩写混合字段
            app_cn_py_abb1, app_cn_py_abb2, app_cn_py_abb3, app_cn_py_abb4
        ]
        
        # 3. 遍历所有精准词，去重后写入文件（无序号，格式：app_id\t精准词）
        for word in all_acc_words:
            # 过滤无效数据（None、空字符串、纯空白字符）
            if not word or word == "None" or not word.strip():
                continue
            # 去重判断：当前词未在该app_id的缓存中
            if word not in appid_word_dict[app_id]:
                # 更新缓存，避免重复
                appid_word_dict[app_id].append(word)
                # 写入文件（无序号，仅app_id和精准词，制表符分隔）
                out_file.write(f"{app_id}\t{word}\n")

print(f"【成功】脚本执行完成，输出文件：{out_path}")
