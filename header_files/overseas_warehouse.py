"""SKU ,站点  海外仓属性审核
"""
from datetime import datetime, timedelta
import pandas as pd
from header_files.func import read_password, root_window_path, time_wrapper
from header_files.pandas_sql import GetFromDuckdb, DateGenerator
from header_files.orderlisting import OrderListing
import pymysql
from tqdm import tqdm
import yaml
from math import ceil
import warnings

# 忽略所有的警告
warnings.filterwarnings('ignore')
num = 1000


@time_wrapper
def hwc_logistics(df):
    """海外仓物流属性配置表"""
    df_data = df[['国家', 'SKU']].drop_duplicates(keep='first')
    df_data['SKU'] = df_data['SKU'].astype(
        str).str.replace(r'\((.*)|\s*|\t*', '', regex=True)
    conn1 = pymysql.connect(host='121.37.228.71',
                            port=9030,
                            user='yibai208384',
                            password='XjcT3ImVrb',
                            database='yibai_plan_stock')
    sku_list = df_data['SKU'].dropna().unique().tolist()
    # print(tuple(sku_list[0:2000]))
    df_oversea_logistics = pd.DataFrame()
    count = len(sku_list) // num + 1
    for i in tqdm(range(count)):
        sql1 = f'''SELECT distinct sku AS 'SKU', station_code AS '站点',
        destination_warehouse_code AS '注册仓库', company_name AS '货代公司',
        CASE
            WHEN audit_attr_status = 1 THEN '待审核'
            WHEN audit_attr_status = 2 THEN '已审核'
            ELSE audit_attr_status -- 保持原值，如果物流审核列不是1或2
        END AS '物流审核',
        CASE
            WHEN shipping_status = 0 THEN '待审核'
            WHEN shipping_status = 1 THEN '审核通过'
            WHEN shipping_status = 2 THEN '审核不通过'
            ELSE shipping_status -- 保持原值，如果发运审核列不是0, 1, 或 2
        END AS '发运审核',
        CASE
            WHEN is_risk = 0 THEN '未知'
            WHEN is_risk = 1 THEN '无风险'
            WHEN is_risk = 2 THEN '可能有风险'
            WHEN is_risk = 3 THEN '中度风险'
            WHEN is_risk = 4 THEN '高风险'
            ELSE is_risk -- 保持原值，如果风险等级列不是0, 1, 2, 3, 或 4
        END AS '风险等级',
        CASE
            WHEN publish_attr_id = 0 THEN '待设置'
            WHEN publish_attr_id = 1 THEN '带电订单'
            WHEN publish_attr_id = 2 THEN '敏感订单'
            WHEN publish_attr_id = 3 THEN '普货订单'
            WHEN publish_attr_id = 4 THEN '不可发'
            WHEN publish_attr_id = 6 THEN '非固体'
            ELSE publish_attr_id -- 保持原值，如果刊登属性列不是0, 1, 2, 3, 4, 或 6
        END AS '刊登属性',
        CASE
            WHEN is_commodify_inspection = 0 THEN '未确定'
            WHEN is_commodify_inspection = 1 THEN '不商检'
            WHEN is_commodify_inspection = 2 THEN '商检'
            ELSE is_commodify_inspection-- 保持原值，如果是否商检列不是0, 1, 或 2
        END AS '是否商检',
        CASE
            WHEN is_fumigating  = 0 THEN '未确定'
            WHEN is_fumigating  = 1 THEN '不熏蒸'
            WHEN is_fumigating  = 2 THEN '熏蒸'
            ELSE is_fumigating  -- 保持原值，如果是否熏蒸列不是0, 1, 或 2
        END AS '是否熏蒸'
        FROM yibai_plan_stock.yibai_oversea_logistics_list
        WHERE sku in ({", ".join(["'{}'".format(sku) for sku in sku_list[i*num:(i+1)*num]])})
        -- AND platform_code = 'AMAZON';'''
        oversea_logistics_0 = pd.read_sql(sql1, conn1)
        df_oversea_logistics = pd.concat(
            [df_oversea_logistics, oversea_logistics_0], axis=0)
    df_oversea_logistics = df_oversea_logistics.drop_duplicates(
        subset=['SKU', '站点'], keep='first')
    with open('header_files\\country_codes.yaml', 'r', encoding='utf-8') as stream:
        data = yaml.safe_load(stream)  # 只调用一次，并将结果存储在 data 变量中
        country_code_to_name = data.get(
            'country_code_to_name', None)  # 使用 get 方法，避免 KeyError
        sites_list = data.get('sites_to_names', [])  # 假设 sites_to_names 是一个列表
        sites_dict = {site['code']: site['name'] for site in sites_list}
    df_oversea_logistics['站点'] = df_oversea_logistics['站点'].map(sites_dict)
    warehouse = warehouse_info()
    warehouse_dict = warehouse.set_index('warehouse_code')[
        'warehouse_name'].to_dict()
    df_oversea_logistics['注册仓库'] = df_oversea_logistics['注册仓库'].map(
        warehouse_dict)
    df = df.merge(df_oversea_logistics, how='left', on=['SKU', '站点'])
    return df


def warehouse_info() -> pd.DataFrame:
    duck = GetFromDuckdb(database='mrp_oversea')
    df_temp = read_password(60, f"""with ware as (SELECT x.* FROM yb_stock_center.yb_warehouse x),
            house as (
            SELECT x.id as real_warehouse_id, name as real_warehouse_name,code as real_warehouse_code FROM yb_stock_center.yb_warehouse x
            WHERE real_warehouse_id = id)
            SELECT wa.id as warehouse_id , wa.name as warehouse_name ,wa.code as warehouse_code,wa.type,
            hs.real_warehouse_id,hs.real_warehouse_name as real_warehouse_name,hs.real_warehouse_code as real_warehouse_code,
            enabled,country,create_time
            from ware wa left join house hs on wa.real_warehouse_id = hs.real_warehouse_id;""")
    with open('header_files\\country_codes.yaml', 'r', encoding='utf-8') as stream:
        data = yaml.safe_load(stream)  # 只调用一次，并将结果存储在 data 变量中
        country_code_to_name = data.get(
            'country_code_to_name', None)  # 使用 get 方法，避免 KeyError
    df_temp['warehouse'] = df_temp['country'].map(country_code_to_name)
    df_temp['warehouse'] = df_temp['warehouse'] + '仓'
    duck.to_duckdb(df_temp, 'yb_warehouse', if_exists='replace')
    return df_temp


@time_wrapper
def product_warehours(df):
    """海外仓订单属性"""
    sku_list = df['SKU'].dropna().unique().tolist()
    # num = 2000
    count = ceil(len(sku_list) / num)
    sku_attr = pd.DataFrame()
    for i in tqdm(range(count)):
        sql_attribute_name = f"""
            SELECT a.sku AS "SKU",
            GROUP_CONCAT(DISTINCT b.attribute_name) AS `产品属性`
            FROM yibai_prod_base.yibai_prod_sku_select_attr a
            LEFT JOIN yibai_prod_base.yibai_prod_attributes_tms b ON a.attr_value_id = b.id
            WHERE b.parent_id = 66 and a.attr_type_id=1 and a.is_del=0 
            and a.sku in ({", ".join(["'{}'".format(sku) for sku in sku_list[i*num:(i+1)*num]])})
            GROUP BY a.sku;"""
        sku_attr_team = read_password(67, sql_name=sql_attribute_name)
        sku_attr = pd.concat(
            [sku_attr, sku_attr_team], axis=0)
    # sku_attr = read_password(67, sql_name=sql_attribute_name)
    sku_attr['产品属性'].fillna('未知', inplace=True)
    df = df.merge(sku_attr, on='SKU', how='left')
    df['应勾选仓库'] = df['国家'] + "仓"
    df['是否包含应勾选仓库'] = df.apply(
        lambda x: '是' if str(x['应勾选仓库']) in str(x['产品属性']) else '否',
        axis=1)
    return df


def supplier_check():
    supplier_code = r'E:\\线下资料\\强制b2b供应商清单_tt.xlsx'
    supplier_code_file = pd.read_excel(supplier_code, sheet_name=0)
    supplier_code_file = supplier_code_file[['supplier_code', '是否强制对公']]
    supplier_code_file.rename(
        columns={'supplier_code': '供应商编码'}, inplace=True)  # type: ignore
    return supplier_code_file


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
    # num = 2000
    count = ceil(len(sku_list) / num)
    df_purchase_data = pd.DataFrame()
    for i in tqdm(range(count)):
        sql9 = f'''SELECT sku AS 'SKU', last_price AS "最新采购价(元)",
        case 
            when is_drawback = 0 then '否' else '是' 
        end AS "是否可退税",
        500/(tax_rate/100)*(1+ticketed_point/100)/last_price  as "最小退税标准" ,
        ticketed_point/100 as "开票点",
        case when maintain_ticketed_point = 0 then '没有维护' else '有维护' end AS "是否维护开票点",
        starting_qty AS "最小起订量", supplier_code AS "供应商编码",inside_number AS '整箱箱率',product_name2 as "商品名称",
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


@time_wrapper
def inf_forbidden(df):
    """SKU在各国侵权违禁状态"""
    sku_list = df['SKU'].dropna().unique().tolist()
    # num = 2000
    count = ceil(len(sku_list) / num)
    df_inf = pd.DataFrame()
    for i in tqdm(range(count)):
        sql_inf = f"""select sku as SKU,
            CASE country_code  
                WHEN 'CA' THEN '加拿大'  
                WHEN 'IT' THEN '意大利'  
                WHEN 'FR' THEN '法国'  
                WHEN 'WD' THEN '全球'  
                WHEN 'AU' THEN '澳洲'  
                WHEN 'JP' THEN '日本'  
                WHEN 'PL' THEN '波兰'  
                WHEN 'BR' THEN '巴西'  
                WHEN 'TR' THEN '土耳其'  
                WHEN 'MX' THEN '墨西哥'  
                WHEN 'AE' THEN '阿联酋'  
                WHEN 'BE' THEN '比利时'  
                WHEN 'DE' THEN '德国'  
                WHEN 'ES' THEN '西班牙'  
                WHEN 'SG' THEN '新加坡'  
                WHEN 'SE' THEN '瑞典'  
                WHEN 'NL' THEN '荷兰'  
                WHEN 'US' THEN '美国'  
                WHEN 'SA' THEN '沙特'  
                WHEN 'UK' THEN '英国'  
                WHEN 'CN' THEN '中国'  
                WHEN 'RU' THEN '俄罗斯'  
                WHEN 'TH' THEN '泰国' 
                WHEN 'ID' THEN '印尼' 
                ELSE '未知国家'  
            END AS '国家',
            case risk_grade_type
                when 'III' THEN '3'
                when 'IV' THEN '4'
                when 'V' THEN '5'
                ELSE risk_grade_type 
            end as '侵权等级',
            infringement as '侵权原因'
        from
            yibai_prod_base.yibai_prod_inf_country_grade
        where
            risk_grade_type in ('III', 'IV','V') 
            and sku in ({", ".join(["'{}'".format(sku) for sku in sku_list[i*num:(i+1)*num]])})
            and is_del = 0"""
        sku_attr = read_password(48, sql_inf)
        df_inf = pd.concat(
            [df_inf, sku_attr], axis=0)
    df_inf.drop_duplicates(inplace=True)
    df_inf['侵权信息'] = df_inf['国家'] + ':' + df_inf['侵权等级']
    df_inf = df_inf.groupby(['SKU', '国家']).agg(
        {'侵权信息': lambda x: ' '.join(x), '侵权原因': lambda x: ' '.join(x)}).reset_index()
    df = df.merge(df_inf, on=['SKU', '国家'], how='left')
    df_inf = df_inf[df_inf['国家'] == '全球']
    df_inf['全球侵权'] = '全球侵权'
    df_inf = df_inf[['SKU', '全球侵权']]
    df = df.merge(df_inf, on=['SKU'], how='left')
    return df


@time_wrapper
def inf_forbidden_all(df):
    """SKU违禁状态"""

    sql_inf = f"""select sku as 'SKU',platform_code as '平台',risk_grade_type  as '违禁等级'
    from  yibai_prod_forbidden_grade
    where  risk_grade_type in ('III', 'IV', 'V') and is_del = 0 and LOWER(platform_code) = 'all' """
    df_inf = read_password(48, sql_inf)
    df_inf.drop_duplicates(inplace=True)
    df_inf['违禁信息'] = df_inf['平台'] + ':' + df_inf['违禁等级']
    df_inf = df_inf.groupby('SKU').agg(
        {'违禁信息': lambda x: ' '.join(x)}).reset_index()
    df = df.merge(df_inf, on=['SKU'], how='left')
    return df


@time_wrapper
def hwc_process(df):
    """海外仓关务注册状态"""
    sku_list = df['SKU'].dropna().unique().tolist()
    # num = 2000
    count = ceil(len(sku_list) / num)
    sku_attr = pd.DataFrame()
    for i in tqdm(range(count)):
        sql = f"""-- explain 
        SELECT DISTINCT 
            sku as 'SKU',
            CASE   
                WHEN country_code = 'US' THEN '美国'  
                WHEN country_code = 'IT' THEN '意大利'  
                WHEN country_code = 'SP' THEN '西班牙'  
                WHEN country_code = 'FR' THEN '法国'  
                WHEN country_code = 'DE' THEN '德国'  
                WHEN country_code = 'UK' THEN '英国'  
                WHEN country_code = 'JP' THEN '日本'  
                WHEN country_code = 'CA' THEN '加拿大'  
                WHEN country_code = 'MX' THEN '墨西哥'  
                WHEN country_code = 'AU' THEN '澳洲'  
                WHEN country_code = 'IN' THEN '印度'  
                WHEN country_code = 'AE' THEN '中东'  
                WHEN country_code = 'SG' THEN '新加坡'  
                WHEN country_code = 'NL' THEN '荷兰'  
                WHEN country_code = 'BR' THEN '巴西'  
                WHEN country_code = 'SA' THEN '沙特'  
                WHEN country_code = 'SE' THEN '瑞典'  
                WHEN country_code = 'PL' THEN '波兰'  
                WHEN country_code = 'TR' THEN '土耳其'  
                WHEN country_code = 'ZH' THEN '中国'    
                WHEN country_code = 'VN' THEN '越南'    
                WHEN country_code = 'UY' THEN '乌拉圭'    
                WHEN country_code = 'TH' THEN '泰国'    
                WHEN country_code = 'RU' THEN '俄罗斯'    
                WHEN country_code = 'PH' THEN '菲律宾'    
                WHEN country_code = 'PE' THEN '秘鲁'    
                WHEN country_code = 'MY' THEN '马来西亚'    
                WHEN country_code = 'ID' THEN '印度尼西亚'    
                WHEN country_code = 'GB' THEN '英国'    
                WHEN country_code = 'ES' THEN '西班牙'    
                WHEN country_code = 'CZ' THEN '捷克'    
                WHEN country_code = 'CS' THEN '捷克'    
                WHEN country_code = 'CN' THEN '中国'    
                WHEN country_code = 'CL' THEN '智利'    
                WHEN country_code = 'BE' THEN '比利时'    
                WHEN country_code = 'AR' THEN '阿根廷'    
                WHEN country_code = 'AQ' THEN '南极' 
                WHEN country_code = 'CO' THEN '哥伦比亚' 
            ELSE country_code  
            END AS "国家",
            case
                company
                when 'WYT' THEN '万邑通'
                when 'GCHWC' THEN '谷仓'
                WHEN 'Hw4PX' THEN '递四方'
                WHEN 'CK1HWC' THEN '出口易'
                WHEN 'HZLZB' THEN '来赞宝'
                WHEN 'CNHWC' THEN '菜鸟'
                WHEN 'FN' THEN '飞鸟'
                WHEN 'Wjfx' THEN '旺集'
                WHEN 'AML' THEN '艾姆乐'
                WHEN 'MBB' THEN 'MBB'
                WHEN 'SD' THEN '三迪'
                WHEN 'GO' THEN '国欧'
                WHEN 'LD' THEN '立德'
                ELSE company
            END as '货代',
            message as '注册信息',
            case
                reg_status
                when '0' THEN '未注册'
                when '1' THEN '注册成功'
                when '2' THEN '注册失败'
                when '3' THEN '待注册'
                when '4' THEN '注册中'
                else reg_status
            end '注册状态',
            case 
                when (reg_status='2' AND remarks !='') THEN remarks
                when (reg_status='2' AND remarks ='' AND return_reason !='') THEN return_reason
                WHEN (reg_status='2' AND remarks ='' AND return_reason ='') THEN message
                ELSE case
                    reg_status
                    when '0' THEN '未注册'
                    when '1' THEN '注册成功'
                    when '2' THEN '注册失败'
                    when '3' THEN '待注册'
                    when '4' THEN '注册中'
                    else reg_status
                END 
            END as '注册结果',
            reg_time as '注册时间'
        FROM
            yibai_tms_logistics.yibai_prod_logistics_register x
        WHERE reg_status !=0 and sku in ({", ".join(["'{}'".format(sku) for sku in sku_list[i*num:(i+1)*num]])})
        order by reg_time DESC """
        sku_attr_team = read_password(69, sql_name=sql)
        sku_attr = pd.concat([sku_attr, sku_attr_team], axis=0)
    sku_attr = sku_attr.drop_duplicates(
        subset=['SKU', '国家', '货代'], keep='first')
    df = df.merge(sku_attr, on=['SKU', '国家', '货代'], how='left')
    return df


@time_wrapper
def oversea_order(start_date, end_date):
    """
    Args:
        start_date (datetime):
        end_date (datetime): 
    """
    duck = GetFromDuckdb(database='oversea')
    order = OrderListing(start_date, end_date)
    df = order.oversea_order_info()
    date_generator = DateGenerator(start_date, end_date)
    list_date = date_generator.generate_dates_between()
    for i in list_date:
        duck.drop_parquet('oversea_order_sum_quantity', i)
    duck.to_parquet_hive(df, 'oversea_order_sum_quantity',
                         partition_cols='update_time')


@time_wrapper
def order_profit_rate(df_inf):
    today = datetime.today().strftime('%Y-%m-%d')
    today_7 = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    today_30 = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    today_60 = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    # oversea_order(datetime.strptime(today_30, '%Y-%m-%d'), datetime.today())
    duck = GetFromDuckdb(database='oversea')
    # print(df_inf)
    df_temp = duck.exe_duckdb(f"""select *
        from read_parquet('E:/duck/parquet_file/oversea/parquet_file/oversea_order_sum_quantity/*/*.parquet',
        hive_partitioning=True)
        where update_time >= '{today_60}' and update_time < '{today}'""")
    duck.to_duckdb(df_temp, 'oversea_order_detail')
    # sum(true_profit_new1)/sum(total_price) as "近30天TEMU平台平均利润率"
    sql = f"""select sku as "SKU",warehouse,mean(profit_rate) as "近30天TEMU平台平均利润率",
        SUM(true_profit_new1)/sum(total_price) "近30天TEMU平台平均利润率1",
        SUM(CASE WHEN update_time>'{today_7}' THEN sku_quantity ELSE 0 end)/7 AS "近7天日均销量" 
        from oversea.main.oversea_order_detail
        where update_time >= '{today_30}' and update_time < '{today}'
        and sku in ({", ".join(["'{}'".format(sku) for sku in df_inf['SKU'].unique()])})
        group by sku,warehouse;"""
    df = duck.exe_duckdb(sql)
    df['国家'] = df['warehouse'].replace('仓', '', regex=True)
    df['国家'] = df['国家'].replace('捷克', '德国', regex=True)
    df['国家'] = df['国家'].replace('澳大利亚', '澳洲', regex=True)
    # 定义分段区间
    bins = [0, 1, 2, 4, 10, float('inf')]
    labels = ['A.[0-1)', 'B.[1-2)', 'C.[2-4)', 'D.[4-10)', 'E.10+']
    # 使用 pandas.cut 进行分段
    df_inf = df_inf.merge(df[['SKU', '国家', '近30天TEMU平台平均利润率',
                              '近30天TEMU平台平均利润率1', '近7天日均销量']], on=['SKU', '国家'], how='left')
    df_inf['近7天日均销量'] = df_inf['近7天日均销量'].fillna(0)
    df_inf['近7天日均销量分段'] = pd.cut(df_inf['近7天日均销量'], bins=bins, labels=labels)
    return df_inf


@time_wrapper
def partner_sku(df):
    duck = GetFromDuckdb(database='oversea_partner_age')
    sql = f"""SELECT DISTINCT 
        "第十三版-责任人",
        x.sku as "SKU",
        case
            when "站点" in ('澳大利亚', '澳洲') then '澳洲'
            ELSE "站点"
        end "站点"
    FROM
        oversea_partner_age.main."海外仓常规---SKU+国家+责任人明细" x
    union
    SELECT DISTINCT 
        "第十三版-责任人",
        x.sku as "SKU",
        case
            when "站点" in ('澳大利亚', '澳洲') then '澳洲'
            ELSE "站点"
        end "站点"
    FROM
        oversea_partner_age.main."盲发---SKU+目的国加权数量（去重）" x"""
    sku_partner_sku = duck.exe_duckdb(sql)
    sku_partner_sku['国家'] = sku_partner_sku['站点'].astype(
        str).replace('欧洲', '德国', regex=True)
    del sku_partner_sku['站点']
    df = df.merge(sku_partner_sku, on=['SKU', '国家'], how='left')
    sql_11 = f"""
        with aa as (SELECT * FROM mrp_oversea.main.oversea_sku_block_guess
        union 
        SELECT * FROM mrp_oversea.main.oversea_sku_block_guess_sep)
        select sku as "SKU","国家","不备货原因" from aa ;"""
    duck_mrp = GetFromDuckdb(database='mrp_oversea')
    duck_mrp = duck_mrp.exe_duckdb(sql_11)
    df = df.merge(duck_mrp, on=['SKU', '国家'], how='left')
    return df


@time_wrapper
# def order_
def main():
    file_name = root_window_path()
    outfile_name = file_name
    df = pd.read_excel(file_name, sheet_name=0)
    # print(df)
    df['SKU'] = df['SKU'].astype(str).str.replace(
        r'\((.*)|\s*|\t*', '', regex=True)
    df = df[df['SKU'] != '']
    dist_map = {
        '加拿大': '加拿大',
        '美东': '美国',
        '美西': '美国',
        '美国': '美国',
        '德国': '德国',
        '英国': '英国',
        '悉尼': '澳洲',
        '墨尔本': '澳洲',
        '澳洲': '澳洲',
        '俄罗斯': '俄罗斯',
        '墨西哥': '墨西哥',
        '菲律宾': '菲律宾',
        '泰国': '泰国',
        '捷克': '德国'
    }
    df['国家'] = df['站点'].map(dist_map)
    df.loc[df['国家'].isna(), '国家'] = df.loc[df['国家'].isna(), '站点']
    with open('header_files\\country_codes.yaml', 'r', encoding='utf-8') as stream:
        data = yaml.safe_load(stream)
        freight_forwarding = data.get('freight_forwarding', 'None')
        df['货代'] = df['国家'].map(freight_forwarding)
    # df = order_profit_rate(df)
    # df = partner_sku(df)
    df = hwc_process(df)
    df = product_warehours(df)
    df = hwc_logistics(df)
    df = inf_forbidden(df)
    df = inf_forbidden_all(df)
    df = product_sku(df)
    df.to_excel(outfile_name, index=False)
    # print(outfile_name)


if __name__ == '__main__':
    main()
