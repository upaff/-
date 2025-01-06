# author:marmot
import asyncio
import csv
import os
import pathlib
import re
import time
import warnings  # 导入警告模块
import duckdb
import pandas as pd
import yaml
from clickhouse_sqlalchemy import make_session
from clickhouse_driver import Client
from sqlalchemy import create_engine
from datetime import datetime, timedelta
# 忽略所有的警告
warnings.filterwarnings('ignore')
save = r'E:\duckdb_database/'
# 共享文件路径
share_save = r'E:\duck\parquet_file'
# share_save = r'E:\duck\duckdb_database/'


def convert_date_format(date_str):
    if isinstance(date_str, datetime):
        date_str = date_str.strftime('%Y-%m-%d')
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


class GetFromDuckdb():
    """
    Info: 获取duckdb数据
    """

    def __init__(self,
                 save_dir=save,
                 database: str = 'send_fba',
                 file_save=None) -> None:
        self.file = pathlib.Path(save_dir)
        self.create_dir(self.file)
        self.db_name = str(pathlib.Path(self.file, database + '.duckdb'))
        self.file_save = pathlib.Path(
            share_save, database,
            'parquet_file') if not file_save else pathlib.Path(file_save, database,
                                                               'parquet_file')

        self.create_dir(self.file_save)
        self.tesk_dir = pathlib.Path('D:/Desktop')

    def to_parquet(self, df, file_name):
        if not pathlib.Path.exists(self.file_save):
            pathlib.Path.mkdir(self.file_save)
        self.file_name = file_name + ".parquet"
        self.file_path = pathlib.Path(self.file_save, self.file_name)
        if type(df) == str:
            sql = f""" copy ({df}) to '{self.file_path}'
            (FORMAT 'parquet', COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000); """
            self.exe_duckdb(sql)
        else:
            sql = f""" copy (select * from df) to '{self.file_path}' 
            (FORMAT 'parquet', overwrite_or_ignore 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000); """
            self.exe_duckdb(sql)

    def to_parquet_hive(self, df, file_name, partition_cols):
        if not pathlib.Path.exists(self.file_save):
            pathlib.Path.mkdir(self.file_save)
        self.file_name_hive = file_name
        self.file_path = pathlib.Path(self.file_save, self.file_name_hive)
        if type(df) == str:
            sql = f""" copy ({df}) to '{self.file_path}'
            (FORMAT 'parquet', PARTITION_BY {partition_cols}, 
            COMPRESSION 'ZSTD', OVERWRITE_OR_IGNORE 1, ROW_GROUP_SIZE 100000); """
            self.exe_duckdb(sql)
        else:
            sql = f""" copy df to '{self.file_path}' 
            (FORMAT 'parquet', PARTITION_BY {partition_cols},
            OVERWRITE_OR_IGNORE 1 , COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000); """
            self.exe_duckdb(sql)

    def create_dir(self, dir):
        if not pathlib.Path.exists(dir):
            pathlib.Path.mkdir(dir, parents=True)

    def read_csv(self, read_name: str) -> pd.DataFrame:
        """
        Args:
            read_name: name of table
        return:
            DataFrame
        """
        self.file_name = str(read_name)
        df = duckdb.read_csv(self.file_name, parallel=True, header=True).df()
        return df

    def to_csv(self, df, file_name, save_type=0) -> None:
        """
        Args:
            df: DataFrame
            read_name: name of table
        """
        _csv_dir = self.file_save if save_type == 1 else self.tesk_dir
        if type(df) == str:
            sql = f""" copy ({df}) to '{pathlib.Path(_csv_dir, file_name+'.csv')}'
            with (header 1); """
            self.exe_duckdb(sql)
        else:
            sql = f""" copy df to '{pathlib.Path(_csv_dir, file_name+'.csv')}' 
            with (header 1); """
            self.exe_duckdb(sql)

    def read_duckdb(self, table_name: str):
        # print(self.db_name)
        self.table = table_name
        with duckdb.connect(self.db_name) as conn:
            df = conn.execute(f"""select * from "{self.table}"; """).df()
            conn.commit()
        return df

    def exe_duckdb(self, sql):
        with duckdb.connect(self.db_name) as conn:
            df = conn.execute(sql).df()
            conn.commit()
        # print(self.db_name)
        return df

    def to_duckdb(self, df, table_name: str, if_exists='replace'):
        self.table = table_name
        if df.empty:
            print(f'{self.table} is empty')
            return
        with duckdb.connect(self.db_name) as conn:
            if if_exists == 'replace':
                sql = f"""create or replace table "{self.table}" as select * from df; """
                conn.execute(sql)
            else:
                try:
                    sql = f'select * from "{self.table}" limit 1; '
                    self.exe_duckdb(sql)
                    sql = f"""insert into "{self.table}" by name (select * from df); """
                    conn.execute(sql)
                except:
                    sql = f"""create table if not exists "{self.table}" as select * from df; """
                    conn.execute(sql)
            conn.commit()

    def get_row_num(self, table_name):
        sql = f""" select 
        estimated_size 
        from duckdb_tables() 
        where table_name = '{table_name}' """
        data_num = self.exe_duckdb(sql)
        return data_num['estimated_size'].to_list()[0]

    def truncate_table(self, table_name):
        judge_num = self.get_table_exists(table_name)
        if judge_num == 1:
            sql = f""" delete from "{table_name}" where 1 """
            self.exe_duckdb(sql)
            print(f'已清空{table_name}')
        else:
            print(f'库中无{table_name}')

    def get_table_exists(self, table_name):
        sql = f""" 
        select 
        * 
        from duckdb_tables() 
        where table_name = '{table_name}' """
        data = self.exe_duckdb(sql)
        if len(data) > 0:
            return 1
        else:
            return 0

    def drop_parquet(self, file_name, contains_str):
        dir_del = self.file_save.joinpath(file_name)
        for i in list(pathlib.Path.glob(dir_del, f'**/*{contains_str}*')):
            for j in list(pathlib.Path.iterdir(i)):
                j.unlink()


class DateGenerator:
    def __init__(self, start_date, end_date) -> None:
        """
        生成指定起始日期和结束日期之间的日期序列
        输出的结果包含 start_year: int, start_month: int, end_year: int, end_month: int
        :param start_year: 起始年份
        :param start_month: 起始月份
        :param end_year: 结束年份
        :param end_month: 结束月份
        """
        self.start_date = convert_date_format(start_date)
        self.end_date = convert_date_format(end_date)
        self.start_year, self.start_month, self.start_day = self.start_date.split(
            '-')  # type: ignore
        self.end_year, self.end_month, self.end_day = self.end_date.split(
            '-')  # type: ignore

    def __iter__(self) -> 'DateGenerator':
        """
        返回日期生成器的迭代器
        :return: 日期生成器的迭代器
        """
        return self._generate_dates()     # type: ignore

    def _generate_dates(self):
        self.current_year = int(self.start_year)
        self.current_month = int(self.start_month)

        while True:
            yield self.current_year, self.current_month
            self.current_month += 1
            if self.current_month > 12:
                self.current_month = 1
                self.current_year += 1
            if self.current_year > int(self.end_year) or (self.current_year == int(self.end_year) and self.current_month >= int(self.end_month)):
                break

    def generate_dates_between(self):
        """
        Parameters:
        start_date (str): The starting date in 'YYYY-MM-DD' format.
        end_date (str): The ending date in 'YYYY-MM-DD' format.
        Returns:
        list[str]: A list containing all dates between the start and end dates (inclusive) as strings in 'YYYY-MM-DD' format.
        """
        start_date_obj = datetime.strptime(self.start_date, '%Y-%m-%d').date()
        end_date_obj = datetime.strptime(self.end_date, '%Y-%m-%d').date()
        dates_list = []
        current_date = start_date_obj
        while current_date < end_date_obj:
            dates_list.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        return dates_list


def ck_read(client, x) -> pd.DataFrame:
    # client = Client(host='124.71.13.250', port='9001', user='yaozixiang', password='y34ao_ziXi654Ang', database='send_fba')
    data, columns = client.execute(x, columnar=True, with_column_types=True)
    df = pd.DataFrame(
        {re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
    return df


def path_dataframe(dataframe, folder_path, file_name) -> None:
    # 判断文件夹是否存在，不存在则创建
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        # 获得文件的扩展名
    extension = os.path.splitext(file_name)[1]
    base_name = os.path.splitext(file_name)[0]  # 获取不带扩展名的文件名
    file_csv = os.path.join(folder_path, base_name + '.csv')
    file_xlsx = os.path.join(folder_path, base_name + '.xlsx')
    file_parquet = os.path.join(folder_path, base_name + '.parquet')
    if not extension:
        extension = ''
    # 根据扩展名或缺乏扩展名的情况保存文件
    if extension == '.csv':
        dataframe.to_csv(file_csv, index=False)
    elif extension == '.xlsx' or extension == '.xls':
        dataframe.to_excel(file_xlsx, sheet_name='明细', index=False)
    elif extension == 'parquet':
        dataframe.to_parquet(file_parquet, index=False)
    else:
        # 如果扩展名不是.csv或.xlsx，则分别保存为两种格式
        dataframe.to_csv(file_csv, index=False)
        dataframe.to_excel(file_xlsx, index=False)


if __name__ == '__main__':
    duck = GetFromDuckdb(database='oversea')
    # df_temp['total_price'] = df_temp['total_price'].astype('float')
    # duck.to_parquet_hive(df_temp,
    #                      file_name='order_list',
    #                      partition_cols='month')
    date_generator = DateGenerator('2024-06-01', '2024-08-05')
    for year, month in date_generator._generate_dates():
        print(year, month, end='\t')
    # print(date_generator.generate_dates_between())
    # E:\duck\parquet_file\oversea\parquet_file\oversea_order_sum_quantity
    duck.drop_parquet(file_name='oversea_order_sum_quantity',
                      contains_str=date_generator.generate_dates_between())
    # duck = GetFromDuckdb(database='send_fba')
    # sql = f"""
    # select account_id,seller_sku,sale,asin,purchase_time
    # from read_parquet('Z:/duck/season_model/parquet_file/order_day_details/*/*.parquet',hive_partitioning=True)
    # where purchase_time > '2024-04-03' and plat_way = 'FBA' and profit_type = '1';"""
    # df = duck.exe_duckdb(sql)
    # duck.to_parquet(df, file_name='purchase_time')
    # print(df.head(1))
