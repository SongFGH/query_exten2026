import sys

pre_appid = None
pre_line = None

for line in sys.stdin:
    line = line.strip()
    if line == '' or line == None:
        continue
    splitinfo = line.split("\t")
    if len(splitinfo) != 23:
        continue
    [
        app_id,
        raw_app_name,
        app_nm_pre,
        app_nm_py1, app_nm_py2, app_nm_py3, app_nm_py4,
        app_nm_abb1, app_nm_abb2, app_nm_abb3,
        app_nm_py_abb1, app_nm_py_abb2, app_nm_py_abb3,
        app_cn_py_m1, app_cn_py_m2, app_cn_py_m3, app_cn_py_m4, app_cn_py_m5, app_cn_py_m6,
        app_cn_py_abb1, app_cn_py_abb2, app_cn_py_abb3, app_cn_py_abb4
    ] = splitinfo

    if app_id != pre_appid:
        if pre_appid:
            print(pre_line)
        pre_appid = app_id
        pre_line = line

if pre_appid:
    print(pre_line)
