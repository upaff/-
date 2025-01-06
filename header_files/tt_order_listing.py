from clickhouse_driver import Client
from datetime import datetime
from matplotlib.dates import relativedelta
from oversea_hwc.pandas_sql import GetFromDuckdb, DateGenerator
from header_files.func import ck_read,  warehouse_info


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


class TT_order_listing:
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
        self.client = Client(host='124.71.34.226', port='16230', user='yaoxiu',
                             password='y78GgfAo_Xi98u', database='jtomtoperp_sync')
        self.duck_order = GetFromDuckdb(
            save_dir=r"E:\duckdb_database/", database='tt_order')
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

    def exe_order_sql(self, function):
        sql_ck = function()
        try:
            df_ck = ck_read(self.client, sql_ck)
            if not df_ck.empty:
                self.result = df_ck.dropna()
            else:
                print('数据为空')
        except Exception as e:
            print(e)
            # 输出错误信息
        return self.result

    def tt_order_sql(self):
        """生成订单查询的sql语句"""
        sql_ck = f"""
            with oms_order_detail as (
            select * from tt_oms_sync.tt_oms_order_detail
            where order_id in (select order_id from tt_oms_sync.tt_oms_order 
            where payment_status=1 and order_status <> 80 and warehouse_id not in (785,478,481,60,323,1000075,100002078,100002079,100002080,100002279,100002312,100002313,100002314,100002315,100002316,100002370,100002371)
            and date(ship_time) >='{self.start_date}' and date(ship_time)  <'{self.end_date}'
            )
            ),
            oms_order as (
            select * from tt_oms_sync.tt_oms_order
            where payment_status=1 and order_status <> 80
            and warehouse_id not in (785,478,481,60,323,1000075,100002078,100002079,100002080,100002279,100002312,100002313,100002314,100002315,100002316,100002370,100002371)
            and order_id in (select order_id from oms_order_detail)
            ),
            oms_order_sku as (
            select * from tt_oms_sync.tt_oms_order_sku
            where order_detail_id in (select id from oms_order_detail)
            ),
            order_profit_df as (
                select order_id,profit,profit_rate,true_profit_rate_new1
                from jtomtoperp_sync.tt_oms_order_profit
                where order_id in (select order_id from oms_order_detail)
            ),
            warehouse as (
            select id as warehouse_id,warehouse_type,warehouse_name as warehouse_name_1
            from tt_logistics_tms_sync.tt_warehouse
            where warehouse_type in (2, 3)
            union all 
            select toInt64(concat('10000', toString(id))) as warehouse_id,warehouse_type,warehouse_name as warehouse_name_1
            from tt_logistics_tms_sync.tt_warehouse_tt
            where warehouse_type in (8)
            ),
            order_all as (
            select  
            a.order_id as order_id
            ,b.platform_order_id as platform_order_id
            ,b.account_id as account_id
            ,a.seller_sku as seller_sku
            ,d.sku as sku
            ,b.platform_code as platform_code
            ,d.quantity as quantity
            ,b.purchase_time as purchase_time
            ,b.payment_time as payment_time
            ,date(b.payment_time) as update_time
            ,b.ship_country_name as ship_country_name
            ,b.ship_country AS ship_country
            ,b.warehouse_id as warehouse_id
            ,b.is_ship_process as is_ship_process
            ,b.platform_status as platform_status
            ,toFloat64(a.total_price) as total_price
            ,f.profit as profit
            ,if(ifNull(f.true_profit_rate_new1,'0')='0',f.profit_rate,f.true_profit_rate_new1) as profit_rate
            from oms_order_detail a
            left join oms_order b ON a.order_id=b.order_id
            left join oms_order_sku d on d.order_detail_id=a.id
            left join order_profit_df f on a.order_id = f.order_id
            where warehouse_id in (select warehouse_id from warehouse) )
            select * from order_all
            ;"""
        # print(sql_ck)
        return sql_ck

    def month_order(self):
        """根据时间拉取销量"""
        result = self.exe_order_sql(function=self.tt_order_sql)
        self.duck_order.to_parquet_hive(
            result, f"tt_order", partition_cols='update_time')
        print(result.info())
        return result


# def main():
    # date_generator = DateGenerator('2024-12-01', '2024-12-25')
    # for year, month in date_generator.generate_dates_between():
    #     print(year, month, end='\t')
    #     start_date = datetime.strptime(f'{year}-{month}-01', '%Y-%m-%d')
    #     end_date = start_date + relativedelta(months=1)
    #     tt_order = TT_order_listing(start_date=start_date, end_date=end_date)
    #     tt_order.month_order()
    # print("订单信息已生成")
    # tt_order = TT_order_listing(start_date=start_date, end_date=end_date)
    # tt_order.month_order()

if __name__ == '__main__':
    tt_order = TT_order_listing(start_date='2024-12-20', end_date='2025-01-10')
    tt_order.month_order()
    # duck_order = GetFromDuckdb(r"E:\duckdb_database/", database='tt_order')
    # sql = f"""select * from read_parquet('E:/duck/parquet_file/tt_order/*/*.parquet', union_by_name=true) where update_time>='2024-05-01' """
    # df = duck_order.exe_duckdb(sql)
    # df.to_csv(r'C:\\Users\\Administrator\\Desktop\\tt_order.csv')
