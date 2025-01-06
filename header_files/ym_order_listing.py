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


class YM_order_listing:
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
        self.client = Client(host='121.37.249.98', port='9001', user='jinji',
                             password='jifnjiiang_10d6s37', database='yibai_dcm_order_sync')
        self.duck_order = GetFromDuckdb(
            save_dir=r"E:\duckdb_database/", database='ym_order')
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

    def ym_order_sql(self):
        """生成订单查询的sql语句"""
        sql_ck = f"""with '{self.start_date}' as start_day,
            '{self.end_date}' as end_day,
            order_list as (
                select
                    *
                from
                    yibai_dcm_order_sync.dcm_order
                where
                    purchase_time >= start_day
                    and purchase_time < end_day 
                    -- and platform_code not in ('TTS') --剔除tiktok的订单
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
                    ) 
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
                date(a.payment_time) as update_time,
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
        # print(sql_ck)
        return sql_ck

    def month_order(self):
        """根据时间拉取销量"""
        self.result = self.exe_order_sql(function=self.ym_order_sql)
        self.result = self.result[self.result['profit'] != '999999.9999']
        self.duck_order.to_parquet_hive(
            self.result, f"ym_order", partition_cols='update_time')
        return self.result


def main():
    date_generator = DateGenerator('2024-01-01', '2024-12-25')
    for year, month in date_generator._generate_dates():
        print(year, month, end='\t')
        start_date = datetime.strptime(f'{year}-{month}-01', '%Y-%m-%d')
        end_date = start_date + relativedelta(months=1)
        ym_order = YM_order_listing(start_date=start_date, end_date=end_date)
        ym_order.month_order()
    print("订单信息已生成")


if __name__ == '__main__':
    ym_order = YM_order_listing(start_date='2024-12-12', end_date='2024-12-23')
    ym_order.month_order()
    # main()
