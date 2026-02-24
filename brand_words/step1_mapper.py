# coding=utf-8
import sys
import re
import pypinyin
from pypinyin import Style

# 辅助函数：判断单个字符是否为中文字符（匹配 Unicode 汉字区间）
def is_chinese_char(c):
    return re.match(r'[\u4e00-\u9fff]', c) is not None

# 辅助函数：过滤字符串，只保留中文字符，避免非汉字影响拼音转换
def filter_chinese(s):
    if not s or not isinstance(s, str):
        return ""
    return ''.join([c for c in s if is_chinese_char(c)])

def main():
    for line in sys.stdin:
        # 1. 输入数据清洗与格式校验
        line = line.strip()
        if not line:  # 过滤空行
            continue
        splitinfo = line.split("\t")
        if len(splitinfo) != 2:  # 过滤非两列格式的数据
            continue
        appid, app_cn_name = splitinfo
        
        # 2. 基础数据预处理（核心：过滤非汉字，避免拼音列表与汉字列表长度不一致）
        # 原始中文名称
        raw_app_name = app_cn_name
        # 取 "-" 前的前缀作为核心处理对象，兜底空字符串
        app_nm_pre = raw_app_name.split("-")[0] if "-" in raw_app_name else raw_app_name
        # 过滤非中文字符，得到纯中文核心名称
        pure_chinese_name = filter_chinese(app_nm_pre)
        # 极端情况兜底：若过滤后为空，使用原前缀避免后续列表为空
        pure_chinese_name = pure_chinese_name if pure_chinese_name else app_nm_pre
        
        # 3. 生成汉字列表与拼音列表（均做兜底，避免空列表）
        char_list = list(pure_chinese_name)
        pinyin_list = pypinyin.lazy_pinyin(pure_chinese_name, style=Style.NORMAL)
        
        # 兜底处理：避免列表为空，导致后续索引访问异常
        char_list = char_list if char_list else [app_nm_pre] if app_nm_pre else [""]
        pinyin_list = pinyin_list if pinyin_list else [app_nm_pre] if app_nm_pre else [""]
        
        # 4. 定义有效长度（后续所有判断均使用这两个长度，保证一致性）
        char_count = len(char_list)
        py_count = len(pinyin_list)
        
        # 5. 拼音名称词（app_nm_py1, app_nm_py2, app_nm_py3, app_nm_py4）
        app_nm_py1 = "".join(pinyin_list)
        app_nm_py2 = "".join(pinyin_list[:2]) if (char_count >= 2 and py_count >= 2) else app_nm_py1
        app_nm_py3 = "".join(pinyin_list[:-1]) if (char_count >= 2 and py_count >= 2) else app_nm_py1
        app_nm_py4 = "".join(pinyin_list[1:]) if (char_count >= 2 and py_count >= 2) else ""
        
        # 6. 中文简写词（app_nm_abb1, app_nm_abb2, app_nm_abb3）
        app_nm_abb1 = "".join(char_list[:2]) if char_count >= 2 else app_nm_pre
        app_nm_abb2 = "".join(char_list[:-1]) if char_count >= 2 else app_nm_pre
        app_nm_abb3 = "".join(char_list[1:]) if char_count >= 2 else ""
        
        # 7. 应用拼音简写词（app_nm_py_abb1, app_nm_py_abb2, app_nm_py_abb3）
        app_nm_py_abb1 = "".join([p[0] for p in pinyin_list if p])  # 增加 p 非空判断，避免空字符串索引
        app_nm_py_abb2 = "".join([p[0] for p in pinyin_list[:2] if p]) if (char_count >= 2 and py_count >= 2) else app_nm_py_abb1
        app_nm_py_abb3 = "".join([p[0] for p in pinyin_list[1:] if p]) if (char_count >= 2 and py_count >= 2) else ""
        
        # 8. 中文和拼音混合词（app_cn_py_m1 ~ app_cn_py_m6）
        app_cn_py_m1 = char_list[0] + "".join(pinyin_list[1:]) if (char_count >= 2 and py_count >= 2) else app_nm_pre
        app_cn_py_m2 = char_list[0] + "".join(pinyin_list[1:2]) if (char_count >= 2 and py_count >= 2) else app_nm_pre
        app_cn_py_m3 = pinyin_list[0] + "".join(char_list[1:]) if (char_count >= 2 and py_count >= 2) else app_nm_pre
        app_cn_py_m4 = pinyin_list[0] + "".join(char_list[1:2]) if (char_count >= 2 and py_count >= 2) else app_nm_pre
        app_cn_py_m5 = "".join(char_list[:2]) + "".join(pinyin_list[2:]) if (char_count >= 2 and py_count >= 2) else app_nm_pre
        app_cn_py_m6 = "".join(pinyin_list[:2]) + "".join(char_list[2:]) if (char_count >= 2 and py_count >= 2) else app_nm_pre
        
        # 9. 中文和拼音缩写词（app_cn_py_abb1 ~ app_cn_py_abb4）（修复核心报错行）
        app_cn_py_abb1 = char_list[0] + app_nm_py_abb3 if (char_count >= 2 and py_count >= 2) else app_nm_pre
        
        app_cn_py_abb2 = ""
        if char_count >= 2 and py_count >= 2:
            # 双重校验，避免索引越界和空字符串取索引
            py_char = pinyin_list[1][0] if (pinyin_list[1] and len(pinyin_list[1]) >= 1) else ""
            app_cn_py_abb2 = char_list[0] + py_char
        else:
            app_cn_py_abb2 = app_nm_pre
        
        app_cn_py_abb3 = app_cn_py_abb1  # 复用逻辑，保持一致性
        app_cn_py_abb4 = ""
        if char_count >= 2 and py_count >= 1:
            py_first_char = pinyin_list[0][0] if (pinyin_list[0] and len(pinyin_list[0]) >= 1) else ""
            app_cn_py_abb4 = py_first_char + "".join(char_list[1:])
        else:
            app_cn_py_abb4 = app_nm_pre
        
        # 10. 拼接所有结果并输出（保持 \t 分隔，格式与原始需求一致）
        output_columns = [
            appid,
            raw_app_name,
            app_nm_pre,
            # 拼音名称词
            app_nm_py1, app_nm_py2, app_nm_py3, app_nm_py4,
            # 中文简写词
            app_nm_abb1, app_nm_abb2, app_nm_abb3,
            # 拼音简写词
            app_nm_py_abb1, app_nm_py_abb2, app_nm_py_abb3,
            # 中拼混合词
            app_cn_py_m1, app_cn_py_m2, app_cn_py_m3, app_cn_py_m4, app_cn_py_m5, app_cn_py_m6,
            # 中拼缩写词
            app_cn_py_abb1, app_cn_py_abb2, app_cn_py_abb3, app_cn_py_abb4
        ]
        
        # 输出：过滤列中的空字符串（可选，根据业务需求调整），用 \t 拼接
        print("\t".join([str(col) for col in output_columns]))

if __name__ == "__main__":
    main()
