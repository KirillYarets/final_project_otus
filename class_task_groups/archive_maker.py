import os
import oracledb
import psycopg2
import pymysql
import pyspark.pandas as ps

from datetime import datetime, timedelta
from hdfs import InsecureClient
from pandas import DataFrame
from pyspark.sql import SparkSession

from class_task_groups.configs import con_config

report_date = (datetime.now() - timedelta(days=1))
report_month = (datetime.now() - timedelta(days=1)).strftime("%B")

df_year = report_date.year
df_month = report_month
df_day = report_date.day

os.environ['JAVA_HOME'] = '/opt/java/jdk-17.0.2'

spark = SparkSession.builder \
    .config('spark.driver.extraClassPath') \
    .master('local') \
    .appName("Final") \
    .getOrCreate()

cl_hadoop = InsecureClient(f'http://777.77.777.777:9870/', user='hadoopadm')  # airflow 'hadoopadm'
cl_airflow = InsecureClient(f'http://777.77.777.77:9870/', user='airflow')  # airflow 'hadoopadm'


class ArchiveMaker:
    def __init__(self, config, keys, hadoop_dir_type):
        self.config = config
        self.keys = keys
        self.hadoop_dir_type = hadoop_dir_type

    def archivate(self, index):
        try:
            print('Установка соединения с ', con_config['host_postgres'], '....\n')
            connect = psycopg2.connect(
                user=con_config['user_postgres'],
                password=con_config['password_postgres'],
                host=con_config['host_postgres'],
                port=con_config['port_postgres'],
                database=con_config['database_postgres']
            )
            print('Соединение с ', con_config['host_postgres'], 'установлено успешно!\n')

        except Exception as er_gen:
            print('Ошибка:' + str(er_gen))
            quit()

        try:
            print('Подготовка данных для загрузки в таблицу  ', self.config[self.keys[index]]['postgres_table'],
                  '....\n')
            cur = connect.cursor()

            stg_tab = self.config[f'{self.keys[index]}']['postgres_table']
            cur.execute(f'select * from oplati_stage_level.{stg_tab}')

            rows = cur.fetchall()
            df = DataFrame(rows, columns=self.config[f'{self.keys[index]}']['select_columns']).fillna(
                'replace_for_null')

            #print(df)
            print('Данные для загрузки в таблицу  ', self.config[self.keys[index]]['postgres_table'],
                  ' успешно подготовлены\n')

            if df.empty:
                print('DataFrame is empty')
            else:
                tab_name = self.config[self.keys[index]]['postgres_table']
                print(tab_name)
                tab_params = tab_name.partition('_')[2]
                print(tab_params)
                df_n = df.fillna('replace_for_null')
                df_n = df_n.astype(str)
                df_n = ps.from_pandas(df_n)

                cl_hadoop.makedirs(
                    f'/oplati_data/archives/Year_{df_year}/{self.hadoop_dir_type}_data/arch_{tab_params}/{df_month}/Day_{df_day}')
                cl_hadoop.set_permission(
                    f"/oplati_data/archives/Year_{df_year}/{self.hadoop_dir_type}_data/arch_{tab_params}/{df_month}/Day_{df_day}",
                    permission='777')

                df_n.to_parquet(
                    f"webhdfs://777.77.777.77:9870/oplati_data/archives/Year_{df_year}/{self.hadoop_dir_type}_data/arch_{tab_params}/{df_month}/Day_{df_day}/data.parquet",
                    mode='overwrite')  # overwrite
                cl_airflow.set_permission(
                    f"/oplati_data/archives/Year_{df_year}/{self.hadoop_dir_type}_data/arch_{tab_params}/{df_month}/Day_{df_day}/data.parquet",
                    permission='777')
        except oracledb.DatabaseError as errOr:
            print('Ошибка:', errOr)
            quit()
        except pymysql.DatabaseError as errMy:
            print('Ошибка:', errMy)
            quit()
        except Exception as errPo:
            print('Ошибка:' + str(errPo))
            quit()

        connect.commit()
        cur.close()
        connect.close()
