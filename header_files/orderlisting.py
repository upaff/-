import re
import warnings  # 导入警告模块
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import pymysql
from clickhouse_driver import Client
from tqdm import tqdm
from datetime import datetime
from oversea_hwc.pandas_sql import GetFromDuckdb
import concurrent.futures
from typing import List
import os
import sys
from header_files.func import ck_read, mysql_read,  warehouse_info

# 忽略所有的警告
warnings.filterwarnings('ignore')


def pretreatment_orgin(df):
    # amazon 用account_id匹配泛欧后的账号站点
    df['account_id'] = df['account_id'].astype(int)
    sql = """
        SELECT account_id, account_name_eu
        FROM account_group
    """
    # 直接取泛欧之后的标准数据，没有多余站字的困扰
    client_send = Client(host='124.71.13.250', port='9001', user='yaozixiang',
                         password='y34ao_ziXi654Ang', database='send_fba')
    df_group = ck_read(client_send, sql)
    df_group['account_name_eu'].replace('站', '', regex=True, inplace=True)
    df_group['account_name_eu'] = df_group['account_name_eu'].str.lower()
    df = df.merge(df_group, on='account_id', how='left')
    return df


def convert_date_format(date_str):
    if isinstance(date_str, datetime):
        # print("date_str 是 datetime.datetime 对象")
        # 如果是 datetime.datetime 对象，则可以在这里执行转换为字符串的操作
        date_str = date_str.strftime('%Y-%m-%d')
        # print("转换后的日期字符串: ", formatted_date_str)
    # else:
    #     print("date_str 不是 datetime.datetime 对象")
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
    except Exception as e:
        print(f'Invalid date format, please use YYYY-MM-DD:{date_str}', e)
        return None


class OrderListing:
    """时间格式设置为YYYY-MM-DD,需要保证开始时间小于结束时间
    month_order 根据时间拉取销量;"""

    def __init__(self, start_date, end_date=None, seller_sku=None, asin=None) -> None:
        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
        # 时间格式设置为YYYY-MM-DD
        # start_date拆分成年月日
        self.start_date = convert_date_format(start_date)
        self.end_date = convert_date_format(end_date)
        if self.start_date is None or self.end_date is None:
            raise ValueError("Invalid date format, please use YYYY-MM-DD")
        self.year, self.month, self.day = self.start_date.split(
            '-')  # type: ignore
        self.result = None
        self.seller_sku = seller_sku
        self.client = Client(host='121.37.30.78', port='9001', user='yaoziyang',
                             password='ya4o_Ziy45An34g', database='yibai_oms_sync')
        self.duck_order = GetFromDuckdb(r"D:\duckdb_database/", 'order')
        self.asin = asin
        if int(self.year) == datetime.today().year:
            self.year_list = ['', '_archtive']
        else:
            self.year_list = ['', '_archtive',
                              '_2020', '_2021', '_2022', '_2023']
        self.warehouse_info = warehouse_info()
        # print(self.warehouse_info)
        self.warehouse_info_id = self.warehouse_info[self.warehouse_info['type'].isin(
            ('third', 'overseas'))]['warehouse_id'].unique().tolist()

    def order_FBA_sql(self, _add):
        """生成FBA订单的sql语句"""
        sql_ck = f""" with order_list as (select a.order_id, a.account_id,date(a.purchase_time) AS update_time,
                               a.purchase_time,
                               a.warehouse_id, a.is_ship_process, a.platform_status
                               from yibai_oms_sync.yibai_oms_order{_add} a
                               WHERE a.platform_code = 'AMAZON' AND a.warehouse_id = '323'
                               AND a.is_ship_process = 0
                               AND a.order_status IN ( 30, 50, 60, 70 )
                               AND a.order_type IN ( 1, 2, 3 ) AND a.platform_status != 'Canceled'
                               AND a.payment_status = 1 AND a.is_abnormal = 0 AND a.is_intercept = 0
                               AND a.refund_status IN ( 0, 2 )
                               AND splitByChar(' ',toString(a.purchase_time))[1] >= '{self.start_date}' and splitByChar(' ',toString(a.purchase_time))[1] < '{self.end_date}'),
            order_detail as (select
                             b.order_id,
                             b.seller_sku, b.item_id, b.total_price,
                             b.asinval as asin,
                             b.quantity as quantity_old,
                             b.id as order_detail_id
                             from yibai_oms_sync.yibai_oms_order_detail{_add} b
                             where quantity > 0 and order_id in (select order_id from order_list)),
            order_sku as (SELECT
                          c.sku as erp_sku,
                          c.quantity,
                          c.order_detail_id
                          FROM
                          yibai_oms_sync.yibai_oms_order_sku{_add} c
                          WHERE c.order_detail_id in (select order_detail_id from order_detail)),
            list_detail as (
                    select *
                from order_list a right join order_detail b  using(order_id))
            select a.order_id,a.account_id , a.update_time, a.purchase_time,
            a.seller_sku,a.asin,a.quantity_old,b.erp_sku,b.quantity
            from list_detail a
            right join order_sku b using(order_detail_id);"""
        return sql_ck

    def order_OVERSEA_sql(self, _add) -> str:
        order_sql = f"""with B as (SELECT DISTINCT B.order_id,B.platform_order_id,
                    B.platform_code,B.account_id, B.payment_time,date(B.payment_time) as update_time,ship_country,
                    CASE
                        when B.order_status=1 then '下载'
                        when B.order_status=10 then '待确认'
                        when B.order_status=20 then '初始化'
                        when B.order_status=30 then '正常'
                        when B.order_status=40 then '待处理'
                        when B.order_status=50 then '部分出库'
                        when B.order_status=60 then '已出库'
                        when B.order_status=70 then '已完结'
                        when B.order_status=80 then '已取消'
                        ELSE ''
                    END AS complete_status,
                    case
                        when B.order_type=1 then '常规线上平台客户订单'
                        when B.order_type=2 then '线下客户订单'
                        when B.order_type=3 then '线上客户补款单'
                        when B.order_type=4 then '重寄单'
                        when B.order_type=5 then '港前重寄'
                        when B.order_type=6 then '虚拟发货单'
                        ELSE '未知'
                    END AS order_typr,
                        CASE
                        WHEN B.payment_status = 1 THEN '已付款'
                        ELSE '未付款'
                    END AS pay_status,
                        CASE
                        WHEN B.refund_status = 0 THEN '未退款'
                        WHEN B.refund_status = 1 THEN '退款中'
                        WHEN B.refund_status = 2 THEN '部分退款'
                        when B.refund_status = 3 then '全部退款'
                        ELSE ''
                    END AS refound_status,
                    B.warehouse_id
                FROM yibai_oms_sync.yibai_oms_order{_add} B
                WHERE  B.payment_time >= '{self.start_date}'
                and B.payment_time < '{self.end_date}'
                and B.order_status <> 80
                and B.refund_status != 3
                and B.warehouse_id in {self.warehouse_info_id}),
            A as (SELECT DISTINCT A.order_id,A.seller_sku,A.quantity,A.id
                FROM yibai_oms_sync.yibai_oms_order_detail{_add} A
                where A.order_id in (SELECT DISTINCT B.order_id from B)),
            C as (SELECT DISTINCT C.total_price, C.order_id,  C.is_oversea, true_shipping_fee,
                multiIf(  toFloat32(C.true_shipping_fee) > 0, C.true_profit_new1,  
                C.profit_new1 is null , '0',  C.profit_new1 ) as true_profit_new1,
                multiIf(toFloat32(total_price) = 0,0,toFloat32(true_profit_new1)/toFloat32(total_price)) as profit_rate
                FROM yibai_oms_sync.yibai_oms_order_profit{_add} C
                where C.order_id in (SELECT DISTINCT B.order_id from B)),
            F as (SELECT DISTINCT F.sku,F.order_detail_id,F.sales_status as sales_status,
                F.quantity as sku_quantity
                FROM yibai_oms_sync.yibai_oms_order_sku{_add} F
                where F.order_detail_id in (SELECT DISTINCT A.id from A))
            select B.update_time as update_time,B.order_id as order_id ,B.ship_country as ship_country,
                B.warehouse_id as warehouse_id,B.platform_code as platform_code,
                B.account_id as account_id, B.payment_time as payment_time, B.complete_status as complete_status,
                A.seller_sku as seller_sku,A.quantity as seller_sku_quantity,
                case when C.total_price='' then 0
                    else toFloat32(C.total_price) 
                end as total_price,
                C.true_shipping_fee true_shipping_fee,
                case when C.true_profit_new1='' then 0
                    else toFloat32(C.true_profit_new1) 
                end as true_profit_new1,C.profit_rate as profit_rate,
                F.sku as sku,F.sku_quantity as sku_quantity,F.sales_status sales_status
            from B
            left join A ON A.order_id=B.order_id
            left join C ON B.order_id=C.order_id
            LEFT join F on F.order_detail_id=A.id;"""
        return order_sql

    def order_overseas_tt_sql(self, _add) -> str:
        """获取tt订单"""
        """保持add函数一致"""
        tt_sql = f"""with {self.start_date} as start_day,
                {self.end_date} as end_day,
                order_list as (
                    select * from jtomtoperp_sync.tt_oms_order
                    where purchase_time >= start_day and purchase_time < end_day
                        and order_status in (30, 50, 60, 70)
                        and order_type in (1, 2, 3)
                        and UPPER(platform_status) != 'CANCELED'
                        and payment_status = 1
                        and is_abnormal = 0
                        and is_intercept = 0
                        and refund_status in (0, 2)
                        and total_price != 0
                        and warehouse_id in (
                            SELECT stock_id AS warehouse_id FROM jtomtoperp_sync.dim_stock_type_dic WHERE warehouse_type='海外仓'
                            UNION ALL 
                            SELECT id AS warehouse_id FROM jtomtoperp_sync.dim_stock_type_dic a
                            LEFT JOIN jtomtoperp_sync.tt_warehouse b ON concat('YB',a.stock_code)=b.warehouse_code
                            WHERE a.warehouse_type='海外仓' AND id IS NOT NULL AND id!=0
                            )
                        and match(order_id,'-RE') = 0
                        AND account_id != 0
                        AND order_id != ''
                ),
                order_detail_df as (
                    select * from jtomtoperp_sync.tt_oms_order_detail
                    where order_id in (select order_id from order_list)
                ),
                order_sku_df as (
                    select * from jtomtoperp_sync.tt_oms_order_sku
                    where order_id in (select order_id from order_list)
                ),
                order_profit_df as (
                    select order_id,profit,profit_rate,true_profit_rate_new1
                    from jtomtoperp_sync.tt_oms_order_profit
                    where order_id in (select order_id from order_list)
                )
                select
                    a.order_id as order_id,a.platform_order_id as platform_order_id,a.account_id as account_id,b.seller_sku as seller_sku,
                    c.sku as sku,a.platform_code as platform_code,
                    c.quantity as quantity,a.purchase_time as purchase_time,a.payment_time as payment_time,date(a.payment_time) as update_time,
                    a.ship_country as ship_country_name,
                    multiIf(
                    ship_country_name='United States','us',
                    ship_country_name='Russian Federation','ru',
                    ship_country_name='Mexico','mx',
                    ship_country_name IN ('United Kingdom','united kingdom'),'gb',
                    ship_country_name='Japan','jp',
                    ship_country_name='Australia','au',
                    ship_country_name='Canada','ca',
                    ship_country_name='Thailand','th',
                    ship_country_name='Malaysia','my',
                    ship_country_name='Vietnam','vn',
                    ship_country_name='Indonesia','id',
                    ship_country_name='Philippines','ph',
                    ship_country_name IN ('Spain','Italy','Germany','Romania','Slovakia','Lithuania','Hungary','Malta',
                    'Denmark','Portugal','Czech Republic','Greece','Netherlands','Poland','Austria','Belgium','France',
                    'Slovenia','Finland','Bulgaria','Ireland','Latvia','Estonia','Sweden','Norway','Serbia','Croatia',
                    'Luxembourg','Turkey','Switzerland','Cyprus','POLAND','FRANCE','spain','Czech republic','NETHERLANDS',
                    'slovenia','France, Metropolitan','POland','Bosnia and Herzegovina','GERMANY','Monaco'),'eu','') AS ship_country,
                    a.warehouse_id as warehouse_id,a.is_ship_process as is_ship_process,a.platform_status as platform_status,
                    a.total_price as total_price,
                    d.profit as profit,
                    if(ifNull(d.true_profit_rate_new1,'0')='0',d.profit_rate,d.true_profit_rate_new1) as profit_rate
                from
                    order_list a
                right join order_detail_df b on a.order_id = b.order_id
                right join order_sku_df c on b.id = c.order_detail_id
                right join order_profit_df d on a.order_id = d.order_id
                WHERE ship_country!='';"""
        client = Client(host='124.71.34.226', port='16230', user='yaoxiu',
                             password='y78GgfAo_Xi98u', database='jtomtoperp_sync')
        return tt_sql

    def order_overseas_ym_sql(self, _add) -> str:
        """获取ym海外仓订单"""
        sql_ym = f"""with {self.start_date} as start_day,{self.end_date} as end_day,
            order_list as (
                select
                    *
                from
                    yibai_dcm_order_sync.dcm_order
                where
                    purchase_time >= start_day
                    and purchase_time < end_day -- and platform_code not in ('TTS') --剔除tiktok的订单
                    and order_status in (30, 50, 60, 70)
                    and order_type in (1, 2, 3)
                    and platform_status != 'Canceled'
                    and payment_status = 1
                    and is_abnormal = 0
                    and is_intercept = 0
                    and refund_status in (0, 2)
                    and total_price != 0
                    and match(order_id, '-RE') = 0
                    and warehouse_id in (
                        select
                            id
                        from
                            yb_datacenter.yb_warehouse
                        where
                            (
                                `type` IN ('third', 'overseas')
                                AND enabled IN (1)
                                and country not in ('CN')
                            )
                            or id in(481, 854)
                    ) -- union all 
            ),
            warehouse_df as (
                select
                    whid.*
                except
            (real_warehouse_id),
                    yws.name as real_warehouse_name
                from
                    (
                        select
                            id as warehouse_id,
                            name as warehouse_name,
                            `type` as warehouse_type,
                            code as warehouse_code,
                            real_warehouse_id,
                            country as warehouse_country
                        from
                            yb_datacenter.yb_warehouse
                    ) whid
                    left join yb_datacenter.yb_warehouse yws on whid.real_warehouse_id = yws.id
            ),
            order_list_df_with_warehouse_df as (
                select
                    oldf.*,
                    wd.*
                except
            (warehouse_id)
                from
                    order_list oldf
                    inner join warehouse_df wd on oldf.warehouse_id = wd.warehouse_id
            ),
            order_detail_df as (
                select
                    *
                from
                    yibai_dcm_order_sync.dcm_order_detail
                where
                    order_id in (
                        select
                            order_id
                        from
                            order_list_df_with_warehouse_df
                    )
            ),
            order_sku_df as (
                select
                    *
                from
                    yibai_dcm_order_sync.dcm_order_sku
                where
                    order_id in (
                        select
                            order_id
                        from
                            order_list_df_with_warehouse_df
                    )
            ),
            order_profit_df as (
                select
                    order_id,
                    profit,
                    profit_rate,
                    true_profit_rate as true_profit_rate_new1
                from
                    yibai_dcm_order_sync.dcm_order_profit
                where
                    order_id in (
                        select
                            order_id
                        from
                            order_list_df_with_warehouse_df
                    )
            )
            select
                a.order_id as order_id,
                a.platform_order_id as platform_order_id,
                a.account_id as account_id,
                b.seller_sku as seller_sku,
                c.sku as sku,
                b.platform_code as platform_code,
                c.quantity as quantity,
                a.purchase_time as purchase_time,
                a.payment_time as payment_time,
                a.ship_country as ship_country,
                a.ship_country_name as ship_country_name,
                a.warehouse_id as warehouse_id,
                a.is_ship_process as is_ship_process,
                a.platform_status as platform_status,
                a.total_price as total_price,
                a.warehouse_country as warehouse_country,
                a.warehouse_name as warehouse_name,
                a.warehouse_type as warehouse_type,
                a.warehouse_code as warehouse_code,
                a.real_warehouse_name as real_warehouse_name,
                d.profit as profit,
                if(
                    ifNull(d.true_profit_rate_new1, '0') = '0',
                    d.profit_rate,
                    d.true_profit_rate_new1
                ) as profit_rate
            from
                order_list_df_with_warehouse_df a
                right join order_detail_df b on a.order_id = b.order_id
                right join order_sku_df c on b.id = c.order_detail_id
                right join order_profit_df d on a.order_id = d.order_id;"""
        return sql_ym

    def exe_order_sql(self, function):
        processed_dfs = []
        for _add in tqdm(self.year_list, ascii=True, desc=f'{self.year}年{self.month}月销量拉取'):
            sql_ck = function(_add)
            try:
                df_ck = ck_read(self.client, sql_ck)
                if not df_ck.empty:
                    processed_dfs.append(df_ck.dropna())
                else:
                    continue
            except Exception as e:
                print(e)
                print(f'订单读取失败:yibai_oms_sync.yibai_oms_order{_add}')
                # 输出错误信息
                continue
        if len(processed_dfs) == 0:
            print('订单读取失败')
        self.result = pd.concat(processed_dfs)
        del processed_dfs
        self.result = self.result.merge(self.warehouse_info[[
                                        'warehouse_id', 'warehouse_name', 'warehouse_code', 'type', 'warehouse']], how='left', on='warehouse_id')
        return self.result

    def month_order(self):
        """根据时间拉取销量"""
        self.result = self.exe_order_sql(function=self.order_FBA_sql)
        # self.result = self.result.groupby(['account_name_eu', 'seller_sku', 'asin', 'erp_sku', 'month']).agg({'quantity': 'sum'}).reset_index()
        # self.result = self.result.rename(columns={'quantity': 'order_quantity'})
        # sku_price = self.duck_order.exe_duckdb(f"""SELECT sku erp_sku, 最新采购价 FROM sku_new_price""")
        # self.result = self.result.merge(sku_price, on='erp_sku', how='left')
        return self.result

    def process_chunk(self, chunk: List[str], _add: str) -> pd.DataFrame:
        # 根据_add构建SQL查询并执行
        sql_ck = f""" with  order_detail as (select
                                     b.order_id,b.seller_sku, b.item_id, b.total_price, b.asinval as asin, b.quantity as quantity_old, b.id as order_detail_id
                                     from yibai_oms_sync.yibai_oms_order_detail{_add}  b
                                     where b.quantity > 0
                                     AND b.seller_sku IN {tuple([f"{sku}" for sku in chunk])}),
                order_list as (select a.order_id, a.account_id, date(a.purchase_time) as month, a.purchase_time, a.warehouse_id, a.is_ship_process, a.platform_status
                                from yibai_oms_sync.yibai_oms_order{_add} a
                                WHERE  a.warehouse_id = '323' AND a.order_id NOT LIKE '%-RE%'
                                AND a.platform_code = 'AMAZON' AND a.order_type IN ( 1, 2, 3 ) AND a.platform_status != 'Canceled' AND a.refund_status IN ( 0, 2 )
                                AND a.order_status IN ( 30, 50, 60, 70 ) AND a.is_abnormal = 0 AND a.is_intercept = 0 AND a.total_price != 0
                                AND a.order_id in (select order_id from order_detail)
                                HAVING month >= '{self.start_date}' and month < '{self.end_date}'),
                order_sku as (SELECT
                              c.sku as erp_sku,
                              c.quantity,
                              c.order_detail_id
                              FROM
                              yibai_oms_sync.yibai_oms_order_sku{_add}  c
                              WHERE c.order_detail_id in (select order_detail_id from order_detail)),
                list_detail as (
                        select *
                    from order_list a right join order_detail b  using(order_id))
                select a.order_id ,a.account_id , a.month, a.purchase_time,a.seller_sku,a.asin,a.quantity_old,b.erp_sku,b.quantity
                from list_detail a right join order_sku b using(order_detail_id);"""  # 构造针对这部分seller_skus的SQL语句
        client = Client(host='121.37.30.78', port='9001', user='yaoziyang',
                        password='ya4o_Ziy45An34g', database='yibai_oms_sync')
        try:
            data, columns = client.execute(
                sql_ck, columnar=True, with_column_types=True)  # type: ignore
            df = pd.DataFrame(
                {re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
            return df
        finally:
            # 确保在处理完数据后关闭数据库连接
            del client
            # print('查询结束')

    def seller_sku_order(self, duck=False):
        """根据seller_sku拉取FBA销量
        返回字段 groupby(['order_id', 'account_name_eu', 'seller_sku', 'asin', 'month']).agg({'quantity_old': 'sum'})
        """
        if self.seller_sku.empty:  # type: ignore
            print("seller_sku列表为空，请检查代码")
            sys.exit(0)
        site = 2000
        cpu_count = os.cpu_count() or 1
        print('线程数：', cpu_count + 4)
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(32, cpu_count + 4)) as executor:
            futures = []
            with tqdm(range(0, len(self.seller_sku), site),  # type: ignore
                      desc=f"{self.start_date}_{self.end_date}链接订单读取：",
                      colour='blue', ascii=True, ncols=100) as outer_tqdm:

                for i in outer_tqdm:
                    chunk = self.seller_sku[i:i + site]  # type: ignore
                    for _add in self.year_list:
                        futures.append(executor.submit(
                            self.process_chunk, chunk, _add))
            processed_dfs = []
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    processed_dfs.append(result.dropna())
        if len(processed_dfs) == 0:
            print('订单读取失败')
            return
        self.result = pd.concat(processed_dfs)
        self.result = pretreatment_orgin(self.result)
        if duck:
            self.duck_order.to_duckdb(
                self.result, f'{self.start_date}_{self.end_date}_seller_sku_order')
        self.result = self.result.drop_duplicates(
            subset=['order_id', 'account_name_eu', 'seller_sku', 'asin', 'month'], keep='last')
        self.result = self.result.groupby(['order_id', 'account_name_eu', 'seller_sku', 'asin', 'month']).agg({
            'quantity_old': 'sum'}).reset_index()
        # self.result = self.result.rename(columns={'quantity': 'order_quantity'})
        return self.result

    def asin_fbm_order(self):
        """根据asin拉取FBM销量"""
        site = 2000
        processed_dfs = []
        for i in tqdm(range(0, len(self.asin), site), desc=f"链接订单读取：", colour='blue', ascii=True):  # type: ignore
            chunk = self.asin[i:i + site]  # type: ignore
            for _add in tqdm(self.year_list, ascii=True):
                df_ck = pd.DataFrame()
                sql_ck = f"""with order_detail as (select b.order_id,b.seller_sku, b.item_id, b.total_price, b.asinval as asin, b.quantity as quantity_old
                     from yibai_oms_sync.yibai_oms_order_detail{_add}  b
                     where b.quantity > 0
                     AND b.asinval IN {tuple([f"{asin}" for asin in chunk])}),
                     order_list as (select a.order_id, a.account_id, date(a.purchase_time) as month, a.purchase_time, a.warehouse_id, a.is_ship_process, a.platform_status
                                    from yibai_oms_sync.yibai_oms_order{_add} a
                                    WHERE  (a.warehouse_id != '323' or a.is_ship_process !=0) AND a.order_id NOT LIKE '%-RE%'
                                    AND a.platform_code = 'AMAZON' AND a.order_type IN ( 1, 2, 3 ) AND a.platform_status != 'Canceled' AND a.refund_status IN ( 0, 2 )
                                    AND a.order_status IN ( 30, 50, 60, 70 ) AND a.is_abnormal = 0 AND a.is_intercept = 0 AND a.total_price != 0
                                    AND a.order_id in (select order_id from order_detail)
                                    HAVING month >= '{self.start_date}' and month < '{self.end_date}')
                    select a.order_id, a.account_id , a.month, a.purchase_time,b.seller_sku, b.asin,b.quantity_old
                    from order_list a left join order_detail b  using(order_id); """
                try:
                    df_ck = ck_read(self.client, sql_ck)
                    if not df_ck.empty:
                        processed_dfs.append(df_ck.dropna())
                        # 如果数据框不为空，则直接加入processed_dfs
                    else:
                        continue
                except Exception as e:
                    print(f'第{i}次订单读取失败:yibai_oms_sync.yibai_oms_order{_add}')
                    print(e)
                    # print(sql_ck)
                    continue
        if len(processed_dfs) == 0:
            print('订单读取失败')
            return
        self.result = pd.concat(processed_dfs)
        self.result = pretreatment_orgin(self.result)
        # self.result = self.result.groupby(['account_name_eu', 'seller_sku', 'asin', 'erp_sku', 'month']).agg({'quantity': 'sum'}).reset_index()
        # self.result = self.result.rename(columns={'quantity': 'order_quantity'})
        self.duck_order.to_duckdb(
            self.result, f'{self.start_date}_{self.end_date}_FBM_ASIN_order')
        return self.result

    def fba_clear(self, df):
        """负利润加快动销时间段"""
        # start_date = "{}-{:02d}-01".format(year, month)
        # if month >= 12:
        #     end_date = "{}-{:02d}-01".format(year + 1, month - 12 + 1)
        # else:
        #     end_date = "{}-{:02d}-01".format(year, month + 1)
        duck = GetFromDuckdb(r'D:\duckdb_database/', database='mrp_py')
        try:
            df_clear = duck.read_duckdb(f'{self.start_date}month_clear')
        except:
            sql_clear = f"""SELECT a.account_id, a.seller_sku, a.adjustment_priority AS "{self.start_date}月状态", SUM(a.day)  AS "{self.start_date}月负利润加快动销天数"
            from (SELECT x.account_id, x.adjustment_priority, x.seller_sku,
            greatest(toDate(x.start_time), toDate('{self.start_date}')) AS
            intersection_start,
            least(toDate(COALESCE(IF(x.end_time = '', '{self.end_date}', x.end_time), '{self.end_date}')), toDate('{self.end_date}')) AS intersection_end,
            intersection_end - intersection_start AS day
            FROM domestic_warehouse_clear.fba_clear_new x
            WHERE x.adjustment_priority = '负利润加快动销'
            HAVING intersection_start < intersection_end) AS a
            GROUP BY a.account_id, a.seller_sku, a.adjustment_priority"""
            click = Client(host='121.37.30.78', port='9001', user='luomansi',
                           password='lu2om_A56nsdi', database='domestic_warehouse_clear')
            df_clear = ck_read(click, sql_clear)
            df_clear = pretreatment_orgin(df_clear)
            duck.to_duckdb(df_clear, f'{self.start_date}month_clear')
        df_clear = df_clear.groupby(['account_name_eu', 'seller_sku', f'{self.start_date}月状态']).agg(
            {f"{self.start_date}月负利润加快动销天数": 'max'}).reset_index()
        df = df.merge(df_clear, how='left', on=[
                      'account_name_eu', 'seller_sku'])
        df[f'{self.start_date}月状态'] = df[f'{self.start_date}月状态'].fillna(
            "非负利润加快动销")
        df[f'{self.start_date}月负利润加快动销天数'] = df[f'{self.start_date}月负利润加快动销天数'].fillna(
            0)
        # 创建分段的边界
        bins_exhaust = [0, 1, 2, 5, 10, 15, 20, 25, 30, float('inf')]
        # 创建分段的标签
        labels_exhaust = ['A 0', 'B 1', 'C 2--5', 'D 5--10',
                          'E 10--15', 'F 15--20', 'G 20--25', 'H 25--30', 'I 30+']
        df[f'{self.start_date}月负利润加快动销天数分段'] = pd.cut(
            df[f'{self.start_date}月负利润加快动销天数'], bins=bins_exhaust, labels=labels_exhaust, right=False)
        return df

    def sessions_order(self):
        """相应月份ASIN ——seller_sku维度流量"""
        duck_mrp = GetFromDuckdb(r'D:\duckdb_database/', database='mrp_py')
        try:
            df_sessions = duck_mrp.read_duckdb(
                f'traffic_sessions_asin_sku_{self.year}-{self.month}month')
            # if de_sessions.empty:
            #     pass
        except:
            df_sessions_sql = f"""SELECT x.erp_id AS account_id ,x.childAsin as asin ,x.sku as seller_sku ,x.sessions
                                FROM yibai_sale_center_amazon.yibai_amazon_traffic_br_by_asin_sku_month x
                                WHERE x.starttime ='{self.start_date}'"""
            coon = pymysql.Connect(host='121.37.228.71', port=9030, user='yibai210788',
                                   password='uo1uiDBnZF', database='yibai_sale_center_amazon')

            df_sessions = mysql_read(df_sessions_sql, coon)
            duck_mrp.to_duckdb(
                df_sessions, f'traffic_sessions_asin_sku_{self.year}-{self.month}month')
        df_sessions = pretreatment_orgin(df_sessions)
        df_sessions = df_sessions.groupby(['account_name_eu', 'seller_sku', 'asin']).agg({
            'sessions': 'max'}).reset_index()
        df_sessions = df_sessions.rename(
            columns={'sessions': f'{self.year}-{self.month}月人流量'})
        return df_sessions

    def oversea_order_info(self):
        """海外仓订单"""
        # duck_oversea = GetFromDuckdb(database='oversea')
        self.result = self.exe_order_sql(self.order_OVERSEA_sql)
        # self.result = self.result.merge(
        #     self.warehouse_info[['warehouse_id', 'warehouse_name', 'warehouse_code', 'type', 'warehouse']])
        # duck_oversea.to_duckdb(
        #     self.result, f"{self.start_date}_{self.end_date}_order")
        return self.result


if __name__ == '__main__':
    order = OrderListing('2024-12-20', '2025-01-10')
    result = order.oversea_order_info()
    duck = GetFromDuckdb(database='yb_oversea_order')
    duck.to_parquet_hive(result, 'yb_oversea_order',
                         partition_cols='update_time')
