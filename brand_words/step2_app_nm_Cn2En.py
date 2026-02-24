# coding=utf-8
import json
import os
import requests
import time
from deep_translator import MyMemoryTranslator

base_path="/data3/longcanwu/accuracy_word_first_location/online_toufang_20260120/brand_words/data"
in_path = base_path+"/step1_2_appid_nm"
out_path = base_path+"/step2_app_nm_Cn2En"
cache_file = base_path+"/step2_app_nm_Cn2En_cache"

def load_cache(cache_file):
    app_dict = {}
    ofile_cache = open(cache_file, 'r', encoding="utf-8")
    app_list = ofile_cache.readlines()
    app_list = [x.strip() for x in app_list]
    for app_info in app_list:
        app_info_arr = app_info.split('\t')
        if not len(app_info_arr) == 3:
            continue
        appid, appnm, appnm_en = app_info_arr
        app_dict[appid] = app_info
    return app_dict 

def update_cache(app_cache, cache_file):
    intent_cache_file = open(cache_file, 'w',encoding="utf-8")
    for app_id in app_cache.keys():
        intent_cache_file.write(app_cache[app_id] + '\n')
    intent_cache_file.flush()
    intent_cache_file.close()


def translator2(str):
    """
    input : str 需要翻译的字符串
    output：translation 翻译后的字符串
    """
    # API
    url = 'http://fanyi.youdao.com/translate?smartresult=dict&smartresult=rule&smartresult=ugc&sessionFrom=null'
    # 传输的参数， i为要翻译的内容
    key = {
        'type': "AUTO",
        'i': str,
        "doctype": "json",
        "version": "2.1",
        "keyfrom": "fanyi.web",
        "ue": "UTF-8",
        "action": "FY_BY_CLICKBUTTON",
        "typoResult": "true"
    }
    # key 这个字典为发送给有道词典服务器的内容
    response = requests.post(url, data=key)
    # 判断服务器是否相应成功
    if response.status_code == 200:
        # 通过 json.loads 把返回的结果加载成 json 格式
        #print(response.text)
        result = json.loads(response.text)
        #print ("输入的词为：%s" % result['translateResult'][0][0]['src'])
        #print ("翻译结果为：%s" % result['translateResult'][0][0]['tgt'])
        translation = result['translateResult'][0][0]['tgt']
        return translation
    else:
        print("有道词典调用失败")
        # 相应失败就返回空
        return None

def translator(input_str):
    """
    翻译函数：使用Google翻译将中文字符串转为英文
    input : input_str 需要翻译的字符串（兼容空值、特殊字符）
    output：translation 翻译后的字符串，翻译失败返回原字符串
    """
    # 处理空字符串/None，直接返回，避免无效请求
    if not input_str or not isinstance(input_str, str):
        return input_str

    try:
        # 核心翻译逻辑：源语言中文(zh)，目标语言英文(en)
        translation = MyMemoryTranslator(source='zh', target='en').translate(input_str)
        return translation

    except Exception as e:
        # 捕获所有异常，打印错误信息，保证程序不崩溃
        print(f"Google翻译调用失败，错误信息：{str(e)}，原文本：{input_str}")
        # 失败后返回原文本，兼容原有逻辑（可根据需求改为返回None）
        return None


app_cache = {}
if os.path.exists(cache_file):
    app_cache=load_cache(cache_file)

infile = open(in_path,'r', encoding="utf-8")
lines = infile.readlines()

if (os.path.exists(out_path)):
    os.remove(out_path)
out_file = open(out_path, "w", encoding="utf-8")

index=0
for line in lines:
    line = line.strip()
    if not line:
        continue
    split_info = line.split("\t")
    split_info_len = len(split_info)
    if split_info_len != 2:
        continue
    [app_id, app_nm] = split_info
    if app_id in app_cache.keys():
        out_file.writelines(app_cache[app_id]+'\n')
    else:
        try:
            app_nm_english = translator(app_nm)
        except:
            print(f"第{index}行出现了错误")
            continue
        if app_nm_english!=None:
            app_cache[app_id]='\t'.join([app_id, app_nm, app_nm_english])
            out_file.writelines('\t'.join([app_id, app_nm, app_nm_english])+'\n')
            # print('\t'.join([app_id, app_nm, app_nm_english])+'\n')
        else:
            app_cache[app_id]='\t'.join([app_id, app_nm, "None"])
            out_file.writelines('\t'.join([app_id, app_nm, "None"])+'\n')
            # print('\t'.join([app_id, app_nm, "None"])+'\n')
        time.sleep(3)
    index+=1
    if index == 1000:
        break
    if index%1000==0:
        print("translator index:",index)
update_cache(app_cache, cache_file)
out_file.close()
