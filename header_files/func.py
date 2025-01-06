import logging
import os
import re
import warnings  # 导入警告模块
from datetime import datetime, timedelta
from pprint import pprint
from time import time, strftime, localtime
from typing import List, LiteralString
import colorlog
import pandas as pd
from oversea_hwc.pandas_sql import GetFromDuckdb
import pymysql
from clickhouse_driver import Client
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine
import tkinter as tk
from tkinter import filedialog
import yaml
from tqdm import tqdm
from math import ceil

# 忽略所有的警告
warnings.filterwarnings('ignore')

active_date_list = ['2023-07-12', '2023-07-11', '2023-07-13', '2023-10-10', '2023-10-11', '2023-10-12',
                    '2023-11-24', '2023-11-25', '2023-11-26', '2023-11-27', '2023-12-21', '2023-12-22', '2023-12-23', '2023-12-24', '2023-12-25', '2023-12-26', '2023-12-27', '2023-12-28', '2023-12-29', '2023-12-30', '2023-12-31', '2024-01-01', '2024-01-02']


# 这里是为了永远将日志文件夹放在当前工程目录下，而不至于当项目下有多个子目录时
def projectpath():
    # pwd = os.getcwd()
    pwd = r'C:\\Users\\Administrator\\Desktop\\发运异常原因查询'
    while len(pwd.split('\\')) > 6:
        pwd = os.path.dirname(pwd)  # 向上退一级目录
    # print(pwd)
    return pwd


def __logfun(isfile=False):
    # black, red, green, yellow, blue, purple, cyan(青) and white, bold(亮白色)
    log_colors_config = {
        'DEBUG': 'bold_white',
        'INFO': 'bold',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'ngld_red',  # 加bold后色彩变亮
    }
    logger = logging.getLogger()
    # 输出到console
    # logger.setLevel(level=logging.DEBUG)
    # 某些python库文件中有一些DEBUG级的输出信息，如果这里设置为DEBUG，会导致console和log文件中写入海量信息
    logger.setLevel(level=logging.INFO)
    console_formatter = colorlog.ColoredFormatter(
        # fmt='%(log_color)s[%(asctime)s.%(msecs)03d] %(filename)s -> %(funcName)s line:%(lineno)d [%(levelname)s] : %(message)s',
        fmt='%(log_color)s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
        # datefmt='%Y-%m-%d  %H:%M:%S',
        log_colors=log_colors_config
    )
    console = logging.StreamHandler()  # 输出到console的handler
    # console.setLevel(logging.DEBUG)
    console.setFormatter(console_formatter)
    logger.addHandler(console)
    # 输出到文件
    if isfile:
        # 设置文件名
        time_line = strftime('%Y%m%d%H%M', localtime(time()))
        log_path = os.path.join(projectpath(), 'log')
        print(log_path)
        if not os.path.exists(log_path):
            os.mkdir(log_path)
        logfile = log_path + '/' + time_line + '.txt'
        print(logfile)
        # 设置文件日志格式
        filer = logging.FileHandler(logfile, mode='w')  # 输出到log文件的handler
        # filer.setLevel(level=logging.DEBUG)
        file_formatter = logging.Formatter(
            fmt='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d  %H:%M:%S'
        )
        # formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
        filer.setFormatter(file_formatter)
        logger.addHandler(filer)
    return logger


def ck_read(client, x):
    """CK库的读取方式1"""
    """client = Client(host='124.71.13.250', port='9001', user='yaozixiang', password='y34ao_ziXi654Ang', database='send_fba')"""
    data, columns = client.execute(x, columnar=True, with_column_types=True)
    df = pd.DataFrame(
        {re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
    return df


def mysql_read(sql, conn=pymysql.Connect(
        host='190.92.254.57', user='210788', password='DvEmvvsRRZ', database='yibai_order', port=3306)) -> pd.DataFrame:
    """mysql数据库的读取方式"""
    cursor = conn.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    columns = [t[0] for t in cursor.description]
    dff = pd.DataFrame(data=data, columns=columns)
    cursor.close()
    conn.close()
    return dff


def convert_date_format(date_str) -> str | None:
    """转换日期格式将YYYY-MM-DD正确转换为时间格式"""
    """如输入 2023-13-01 转换为2024-01-01"""
    try:
        # 提取年份、月份和日期
        year, month, day = map(int, date_str.split('-'))
        # 修正月份和年份
        if month > 12:
            corrected_month = month % 12
            corrected_year = year + month // 12
        else:
            corrected_month = month
            corrected_year = year
            # 创建新的日期对象
        corrected_date = datetime(corrected_year, corrected_month, day)
        new_date_string = corrected_date.strftime('%Y-%m-%d')
        return new_date_string
    except ValueError:
        print(f'Invalid date format, please use YYYY-MM-DD:{date_str}')
        return None


def path_dataframe(dataframe, folder_path, file_name):
    """将DataFrame保存为csv和xlsx格式的文件, 文件过大建议存csv"""
    # 判断文件夹是否存在，不存在则创建
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        # 获得文件的扩展名
    extension = os.path.splitext(file_name)[1]
    base_name = os.path.splitext(file_name)[0]  # 获取不带扩展名的文件名
    file_csv = os.path.join(folder_path, base_name + '.csv')
    file_xlsx = os.path.join(folder_path, base_name + '.xlsx')
    if not extension:
        extension = ''
    # 根据扩展名或缺乏扩展名的情况保存文件
    if extension == '.csv':
        dataframe.to_csv(file_csv, index=False)
    elif extension == '.xlsx' or extension == '.xls':
        dataframe.to_excel(file_xlsx, sheet_name='明细', index=False)
    else:
        # 如果扩展名不是.csv或.xlsx，则分别保存为两种格式
        dataframe.to_csv(file_csv, index=False)
        dataframe.to_excel(file_xlsx, index=False)


def extract_chinese_characters(text) -> LiteralString:
    # 将数据中文和其他字符分别提取出来
    chinese_characters = re.findall(r'[\u4e00-\u9fff]', text)
    # other_characters = re.findall(r'[^\u4e00-\u9fff]', text)
    chinese_characters = ''.join(chinese_characters)
    return chinese_characters


def split_(identifier):  # -> tuple:
    """拆分标识为bs, account_name, sku, seller_sku, asin"""
    bs = identifier.split('@')[0]
    acc = identifier.split('$')[0]
    se = identifier.split('$')[1]  # 拆分 se 部分
    sk = identifier.split('@')[1]  # 拆分 sk 部分
    asin = identifier.split('$')[2].split('@')[0].split('-')[1]  # 拆分 as 部分
    return bs, acc, sk, se, asin


def pan_european(df) -> pd.DataFrame:
    df['account_name_eu'] = df['account_name_eu'].str.lower()
    df['account_name_eu'] = df['account_name_eu'].replace('站', '')
    return df


def account_id_name() -> pd.DataFrame:
    account_group = ck_read(
        Client(host='124.71.13.250',
               port='9001',
               user='yaozixiang',
               password='y34ao_ziXi654Ang',
               database='send_fba'), f'SELECT * FROM account_group')
    account_group = pan_european(account_group)
    account_group['站点'] = account_group['account_name_eu'].map(
        extract_chinese_characters)
    return account_group


def get_dates_between(start_date, end_date) -> List[pd.Timestamp]:
    # 将输入的字符串转换为datetime对象（假设输入的是"YYYY-MM-DD"格式）
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    # 创建一个包含开始和结束日期之间所有日期的列表
    date_list = pd.date_range(
        start=start_date, end=end_date - pd.Timedelta(days=1)).tolist()
    return date_list


def query_data_from_clickhouse(conf, sql) -> pd.DataFrame:
    connection = 'clickhouse://{user}:{password}@{host}:{port}/{database}'.format(
        **conf)
    engine = create_engine(connection, pool_size=10000,
                           pool_recycle=3600, pool_timeout=20)
    session = make_session(engine)
    cursor = session.execute(sql)
    try:
        fields = cursor._metadata.keys
        df = pd.DataFrame([dict(zip(fields, item))
                          for item in cursor.fetchall()])
    finally:
        cursor.close()
        session.close()
    return df


def read_password(id_int: int, sql_name: str) -> pd.DataFrame:
    """
    根据给定的id从密码表中获取账号和密码信息。
    参数:
    id_int (int): 要查找的数据表ID。
    返回值:
    dict: 包含账号和密码信息的字典，如 {'account': '...', 'password': '...'}。
    如果没有找到对应的数据，则返回None或其他适当默认值。
    Dara
    """
    duck = GetFromDuckdb(save_dir=r'D:\duckdb_database/',
                         database='monitoring')
    sql_pass = f"""SELECT type_name,host ,user_name ,password ,port ,database,tabel_name 
        FROM "database" WHERE id = {id_int};"""
    base = duck.exe_duckdb(sql_pass)
    base.drop_duplicates(inplace=True)
    df_data = pd.DataFrame()
    if base.empty:
        print('\033[31m没有找到该账号的数据库信息')
    elif base['type_name'].iloc[0] == 'ck':
        dist_ck = {
            'user': base['user_name'].iloc[0],
            'password': base['password'].iloc[0],
            'host': base['host'].iloc[0],
            'port': int(base['port'].iloc[0]),
            'database': base['database'].iloc[0]
        }
        if int(base['port'].iloc[0]) == 8123 or int(base['port'].iloc[0]) == 15200:
            print('特殊端口8123')
            df_data = query_data_from_clickhouse(dist_ck, sql_name)
        else:
            df_data = ck_read_new(dist_ck, sql_name)
    elif base['type_name'].iloc[0] == 'mysql':
        dist_mysql = {
            'user': base['user_name'].iloc[0],
            'password': base['password'].iloc[0],
            'host': base['host'].iloc[0],
            'port': int(base['port'].iloc[0]),
            'database': base['database'].iloc[0]
        }
        df_data = mysql_read_new(dist_mysql, sql_name)
    else:
        print('\033[31m该账号的数据库密码错误,请更新库名密码\033[0m')
    return df_data


def ck_read_new(conn_config, sql_):
    """CK库的读取方式1"""
    """client = Client(host='124.71.13.250', port='9001', user='yaozixiang', password='y34ao_ziXi654Ang', database='send_fba')"""
    client = Client(**conn_config)
    data, columns = client.execute(
        sql_, columnar=True, with_column_types=True)  # type: ignore
    df = pd.DataFrame(
        {re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
    return df


def mysql_read_new(conn_config, sql_) -> pd.DataFrame:
    conn = pymysql.Connect(**conn_config)
    cursor = conn.cursor()
    cursor.execute(sql_)
    data = cursor.fetchall()
    columns = [t[0] for t in cursor.description]
    dff = pd.DataFrame(data=data, columns=columns)
    cursor.close()
    conn.close()
    return dff


def logistics_suggested(data):
    """
    处理数据计算发货开始周，修改站点名称，创建辅助列，以及根据物流建议时效表计算物流建议时间。
    参数:
    data: 字典类型，包含发货开始时间、站点、发运方式和订单类型等键值对。
    返回值:
    data: 更新后的字典，包含物流建议时效等新增的键值对。
    """
    # 计算发货开始周，并格式化为"年-周"的形式
    data['发运开始周'] = data['发运开始时间'].map(
        lambda x: f"{x.isocalendar()[0]}-{int(x.isocalendar()[1]):02d}周")
    # 将站点中的"中东"替换为"阿联酋"
    data['站点'].replace('中东', '阿联酋', regex=True, inplace=True)
    # 创建辅助列，用于匹配物流建议时效
    data['辅助列'] = data['站点'] + data['发运方式'] + data['订单类型']
    # 读取物流建议时效表
    Logistics_suggested_time = pd.read_excel(
        'C:\\Users\\Administrator\\Desktop\\物流建议时效表.xlsx', sheet_name='周物流建议时效', engine='openpyxl')
    # 将物流建议时效表设置为以辅助列为索引
    Logistics_suggested_time = Logistics_suggested_time.set_index('辅助列')

    def get_suggested_time(row):
        """
        根据给定的行数据，获取对应的物流建议时间。
        参数:
        row: pandas.Series类型，包含一行数据。
        返回值:
        int: 物流建议时间，如果找不到对应时间，则返回90。
        """
        try:
            return Logistics_suggested_time.loc[row['辅助列'], row['发运开始周']]
        except KeyError:
            # 如果找不到物流建议时间，则打印错误信息，并返回默认值90
            print(f"{row['发运开始周']}数据存在问题")
            return 90
    # 应用get_suggested_time函数，为每行数据计算物流建议时间
    data['物流建议时效'] = data.apply(get_suggested_time, axis=1)
    return data


def complement_asin(df_f):
    """
    补全asin
    参数:df_f: 包含发货开始时间、站点、发运方式和订单类型等键值对。
    返回值:
    data: 更新后的字典，包含物流建议时效等新增的键值对。
    """
    seller_sku = df_f['seller_sku'].dropna().unique().tolist()
    account_name = account_id_name()
    account_order = account_name[['account_id', 'account_name_eu']]
    alls = []
    for i in range(0, len(seller_sku), 2000):
        seller_sku_site = seller_sku[i:i + 2000]
        sql = f"""SELECT x.seller_sku ,x.account_id ,x.asin1 as asin
        FROM yibai_product_kd_sync.yibai_amazon_listing_alls x
        WHERE x.seller_sku IN {tuple(seller_sku_site)}
        """
        # AND x.fulfillment_channel ='AMA'
        df_at1 = ck_read(Client(host='121.37.30.78', port='9001', user='yaoziyang',
                         password='ya4o_Ziy45An34g', database='yibai_product_kd_sync'), x=sql)
        df_at1 = df_at1.merge(account_order, how='left', on=['account_id'])
        del df_at1['account_id']
        df_at1['account_name_eu'].replace(
            '德国|法国|西班牙|意大利|波兰|荷兰|土耳其|比利时|瑞典', '欧洲', regex=True, inplace=True)
        # df_at1.replace(' ', '', regex=True, inplace=True)
        df_at1['account_name_eu'] = df_at1['account_name_eu'].str.lower()
        df_at1.drop_duplicates(['account_name_eu', 'seller_sku', 'asin'])
        df_at1 = df_at1[['account_name_eu', 'seller_sku', 'asin']]
        if df_at1.shape[0] != 0:
            alls.append(df_at1)
    alls_list = pd.concat(alls)
    alls_list = alls_list.drop_duplicates(
        ['account_name_eu', 'seller_sku', 'asin'])
    # duck.to_duckdb(alls_list, 'all_list')
    # alls_list = duck.read_duckdb('all_list')
    df_f = df_f.merge(alls_list, how='left', on=[
                      'account_name_eu', 'seller_sku'])
    return df_f


def convert_datetime_to_string(df) -> pd.DataFrame:
    '''将所有的时间列转换为字符串格式，避免格式出错'''
    for column in df.columns:
        if (df[column].dtype == 'datetime64[ns]'
            or df[column].dtype == 'datetime64[us]'
                or df[column].dtype == 'datetime.date'):
            df[column] = df[column].dt.strftime('%Y-%m-%d')
        else:
            continue
    return df


def time_wrapper(func):  # -> Callable[..., Any]:
    def wrapper(*args, **kwargs):
        # 记录函数开始执行的时间
        start_time = time()
        # 打印函数调用前的信息
        # print(f"Function '{func.__name__}' called with arguments: {args}, {kwargs}")
        # 调用原始函数并获取返回值
        result = func(*args, **kwargs)
        # 计算并记录函数运行结束的时间和总耗时
        end_time = time()
        run_time = end_time - start_time
        duration = timedelta(seconds=run_time)
        hours, remainder = divmod(duration.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"{func.__name__}函数执行时间为:{hours:02d}:{minutes:02d}:{seconds:02d}")
        return result
    return wrapper


def monitot_fba(df) -> pd.DataFrame:
    order_id = df['order_id'].dropna().unique().tolist()
    site_int = 2000
    data = pd.DataFrame()
    for i in range(0, len(order_id), site_int):
        slicing_list = order_id[i:i + site_int]
        sql = f"""
        SELECT x.order_id ,x.seller_sku ,x.`净利润` ,x.`销售额`  ,`净利润` /x.`销售额` as `净利润率` 
        FROM domestic_warehouse_clear.monitor_fba_order x
        where x.order_id in{tuple(slicing_list)}
        """
        client = Client(host='121.37.30.78', port='9001', user='yaoziyang',
                        password='ya4o_Ziy45An34g', database='domestic_warehouse_clear')
        data_temp = ck_read(client, sql)
        if data_temp.empty:
            continue
        data = pd.concat([data, data_temp], axis=0)
    data = data.drop_duplicates(subset=['order_id', 'seller_sku'])
    data['order_type'] = 'FBA'
    df = df.merge(data, how='left', on=['order_id', 'seller_sku'])
    return df


def monitot_fba_fbm(df) -> pd.DataFrame:
    df_1 = df.loc[df['净利润率'].isna()]
    df_0 = df.loc[~df['净利润率'].isna()]
    df_1.drop(columns={'净利润', '销售额', '净利润率', 'order_type'}, inplace=True)
    data = monitot_fbm(df_1)
    """ 根据净利润率升序排列"""
    data = data.sort_values(by="净利润率")
    data = data.drop_duplicates(
        subset=['order_id', 'seller_sku'], keep='first')
    df = pd.concat([df_0, data], axis=0)
    df['净利润率'].fillna(99, inplace=True)
    df['order_type'].fillna('非FBA且非FBM', inplace=True)
    return df


def monitot_fbm(df) -> pd.DataFrame:
    order_id = df['order_id'].dropna().unique().tolist()
    site_int = 2000
    data = pd.DataFrame()
    for i in range(0, len(order_id), site_int):
        slicing_list = order_id[i:i + site_int]
        sql = f"""
        SELECT x.order_id ,x.seller_sku ,x.`净利润` ,x.`销售额`  ,`净利润` /x.`销售额` as `净利润率` 
        FROM domestic_warehouse_clear.monitor_dom_order x
        where x.order_id in{tuple(slicing_list)}
        """
        client = Client(host='121.37.30.78', port='9001', user='yaoziyang',
                        password='ya4o_Ziy45An34g', database='domestic_warehouse_clear')
        data_temp = ck_read(client, sql)
        if data_temp.empty:
            continue
        data = pd.concat([data, data_temp], axis=0)
    data = data.drop_duplicates(subset=['order_id', 'seller_sku'])
    data['order_type'] = 'FBM'
    df = df.merge(data, how='left', on=['order_id', 'seller_sku'])
    return df


def order_classify_row(row) -> str:
    order_type = row['order_type']
    net_profit_rate = row['净利润率']

    # 先处理日期条件
    if row['month'] in (active_date_list):
        return '活动日订单'

    # 根据订单类型和净利润率统一处理
    profit_status = '正利润' if net_profit_rate > 0 else '负利润'

    # 判断订单类型
    if order_type == 'FBA':
        return f'FBA{profit_status}订单'
    else:
        return f'FBM{profit_status}订单'


def order_classify(df) -> pd.DataFrame:
    '''需要信息 account_name_eu, seller_sku, asin, order_id, month'''
    df = monitot_fba(df)
    df = monitot_fba_fbm(df)
    df = convert_datetime_to_string(df)
    df['订单类型'] = df.apply(order_classify_row, axis=1)
    return df


def root_window_path() -> str:
    '''选择目标文件'''
    root = tk.Tk()
    root.withdraw()
    # 获取文件夹路径
    f_path = filedialog.askopenfilename()
    # print('\n获取的文件地址：', f_path)
    return f_path


def read_any_path(file_path):
    """
    根据文件扩展名自动检测文件类型并读取文件。
    支持txt、csv、xlsx。
    """
    extension = file_path.split('.')[-1].lower()
    match extension:
        case 'txt':
            data = pd.read_csv(file_path, sep='\t', encoding='utf-8')
        case 'csv':
            data = pd.read_csv(file_path, encoding='utf-8')
        case 'xlsx', 'xls':
            data = pd.read_excel(file_path)
        case _:
            raise ValueError('不支持的文件格式')
    return data


def cloud_warehouse(df) -> pd.DataFrame:
    """调用实时云仓库存数据"""
    list_sku = df['SKU'].unique().tolist()
    sql = f"""SELECT sku ,warehouse_id ,available_stock as "可用库存" FROM yb_stock_center.yb_stock x
        WHERE cargo_owner_id =8 and sku in {tuple(list_sku)}"""
    df_warehouse = read_password(63, sql)
    df = df.merge(df_warehouse, how='left', on=['SKU', 'warehouse_id'])
    df['可用库存'] = df['可用库存'].fillna(0).astype('int64')
    return df


def warehouse_info():
    duck = GetFromDuckdb(database='oversea')
    df_temp = read_password(60, f"""with ware as (SELECT x.* FROM yb_stock_center.yb_warehouse x),
            house as (
            SELECT x.id as real_warehouse_id, name as real_warehouse_name,code as real_warehouse_code ,country,enabled
                            FROM yb_stock_center.yb_warehouse x
            WHERE real_warehouse_id = id)
            SELECT wa.id as warehouse_id , wa.name as warehouse_name ,wa.code as warehouse_code,wa.type,
            hs.real_warehouse_id,hs.real_warehouse_name as real_warehouse_name,
                            hs.real_warehouse_code as real_warehouse_code,hs.country as country,
            hs.enabled as enabled,create_time
            from ware wa left join house hs on wa.real_warehouse_id = hs.real_warehouse_id;""")
    with open(r'D:\VScode\learn-git\my-repo\header_files\country_codes.yaml', 'r', encoding='utf-8') as stream:
        data = yaml.safe_load(stream)  # 只调用一次，并将结果存储在 data 变量中
        country_code_to_name = data.get(
            'country_code_to_name', None)  # 使用 get 方法，避免 KeyError
    df_temp['warehouse'] = df_temp['country'].map(country_code_to_name)
    df_temp['warehouse'] = df_temp['warehouse'] + '仓'
    duck.to_duckdb(df_temp, 'yb_warehouse', if_exists='replace')
    # df_temp = df_temp[df_temp['enabled'] == 1]
    return df_temp


@time_wrapper
def product_sku(df):
    """SKU信息：最新采购价(元),最小起订量,整箱箱率,外箱尺寸(cm),整箱重量(kg),外箱体积(cm³)"""
    df['SKU'] = df['SKU'].astype(str).str.replace(r'\((.*)|\s*|\t*', '')
    conn6 = pymysql.connect(host='121.37.228.71',
                            port=9030,
                            user='yibai208384',
                            password='XjcT3ImVrb',
                            database='yibai_purchase')
    sku_list = df['SKU'].dropna().unique().tolist()

    def supplier_check():
        supplier_code = r'E:\\线下资料\\强制b2b供应商清单_tt.xlsx'
        supplier_code_file = pd.read_excel(supplier_code, sheet_name=0)
        supplier_code_file = supplier_code_file[['supplier_code', '是否强制对公']]
        supplier_code_file.rename(
            columns={'supplier_code': '供应商编码'}, inplace=True)  # type: ignore
        return supplier_code_file

    num = 1000
    count = ceil(len(sku_list) / num)
    df_purchase_data = pd.DataFrame()
    for i in tqdm(range(count)):
        sql9 = f'''SELECT sku AS 'SKU', last_price AS "最新采购价(元)", starting_qty AS "最小起订量", supplier_code AS "供应商编码",inside_number AS '整箱箱率',product_name2 as "商品名称",
        box_size AS '外箱尺寸(cm)', box_weight AS '整箱重量(kg)', outer_box_volume AS '外箱体积(cm³)',
        sample_package_length as "产品包装长(cm)", sample_package_width "产品包装宽(cm)", sample_package_heigth "产品包装高(cm)", sample_package_weight "产品包装重量(g)" ,
        sample_package_length*sample_package_width*sample_package_heigth/1000000 '产品体积(m³)'
        FROM yibai_purchase.pur_product
        WHERE sku in ({", ".join(["'{}'".format(sku) for sku in sku_list[i*num:(i+1)*num]])}) '''
        purchase_data_0 = pd.read_sql(sql9, conn6)
        df_purchase_data = pd.concat(
            [df_purchase_data, purchase_data_0], axis=0)
    df_purchase_data['SKU'] = df_purchase_data['SKU'].astype(str).str.replace(
        r'\((.*)|\s*|\t*', '')
    df_purchase_data = df_purchase_data.drop_duplicates()
    df_purchase_data.loc[df_purchase_data['整箱箱率'] == 0, '整箱箱率'] = 1
    df_purchase_data.loc[df_purchase_data['整箱箱率'] == '无', '整箱箱率'] = 1
    df_purchase_data.loc[df_purchase_data['整箱箱率'].isnull(), '整箱箱率'] = 1
    df_purchase_data.loc[df_purchase_data['最小起订量'] == 0, '最小起订量'] = 1
    df_purchase_data.loc[df_purchase_data['最小起订量'] == '无', '最小起订量'] = 1
    df_purchase_data.loc[df_purchase_data['最小起订量'].isnull(), '最小起订量'] = 1
    df_supplier = supplier_check()
    df_purchase_data = df_purchase_data.merge(
        df_supplier, on='供应商编码', how='left')
    df_purchase_data['是否强制对公'] = df_purchase_data['是否强制对公'].fillna('非强制对公')
    # df_purchase_data =df_purchase_data[['商品名称','SKU']]
    df = df.merge(df_purchase_data, on='SKU', how='left')
    return df


# def mysql_read(sql, conn=pymysql.Connect(
#         host='190.92.254.57', user='210788', password='DvEmvvsRRZ', database='yibai_order', port=3306)) -> pd.DataFrame:
#     """mysql数据库的读取方式"""
#     cursor = conn.cursor()
#     cursor.execute(sql)
#     data = cursor.fetchall()
#     columns = [t[0] for t in cursor.description]
#     dff = pd.DataFrame(data=data, columns=columns)
#     cursor.close()
#     conn.close()
#     return dff
# 192.168.0.108  3306  sq208402

# if __name__ == '__main__':
#     warehouse_info = warehouse_info()
# sql = f'''SELECT sku,JSON_EXTRACT(product_stock_maps,"$[0]") as json_sku,
#     JSON_EXTRACT(JSON_EXTRACT(product_stock_maps,"$[0]") ,"$.stockCount") as "在库库存",
#     JSON_EXTRACT(JSON_EXTRACT(product_stock_maps,"$[0]") ,"$.stockId") as "仓库code",
#     JSON_EXTRACT(JSON_EXTRACT(product_stock_maps,"$[0]") ,"$.assignedCount") as "可配库存",
#     JSON_EXTRACT(JSON_EXTRACT(product_stock_maps,"$[0]") ,"$.onWayCount") as "在途数量"
#     FROM jtomtoperp.product x
#     WHERE sku !=''
#     and (JSON_EXTRACT(JSON_EXTRACT(product_stock_maps,"$[0]") ,"$.stockCount")>0
#     or JSON_EXTRACT(JSON_EXTRACT(product_stock_maps,"$[0]") ,"$.assignedCount")>0
#     or JSON_EXTRACT(JSON_EXTRACT(product_stock_maps,"$[0]") ,"$.onWayCount") >0)'''
# df = mysql_read(sql=sql, conn=pymysql.Connect(
#     host='192.168.0.108', user='sq208402', password='gMUm9LSkER$./04', database='jtomtoperp', port=3306))
# df.to_csv(r'C:\\Users\\Administrator\\Desktop\\TT海外仓库存20240830.xlsx')
