from class_task_groups.configs import con_config
from class_task_groups.table_validator import (extract_expected_columns_and_date_formats,
                                               validate_dataframe,
                                               write_errors_to_database,
                                               )

from airflow.exceptions import AirflowException
from pandas import DataFrame
from sqlalchemy import create_engine
import oracledb
import pandas as pd
import psycopg2
import pymysql


class DataLoader:
    def __init__(self, config, keys, database_type):
        self.config = config
        self.keys = keys
#        self.index = index
        self.database_type = database_type
        self.con = None
        self.cnxn = None
        self.df = None
        self.failed_rows = None
        self.error_messages = None

    @staticmethod
    def connect_to_out_source(database_type):
        try:
            if database_type == 'bitrix':
                print('Установка соединения с ', con_config['host_mysql'], '....\n')

                con = pymysql.connect(
                    host=con_config['host_mysql'],
                    user=con_config['user_mysql'],
                    password=con_config['password_mysql'],
                    db=con_config['database_mysql'],
                    charset=con_config['charset'],
                    cursorclass=pymysql.cursors.DictCursor
                )

                print('Соединение с ', con_config['host_mysql'], 'установлено успешно!\n')

            elif database_type == 'oracle':
                print('Установка соединения с ', con_config['host_oracle'], '....\n')

                con = oracledb.connect(
                    user=con_config['username_oracle'],
                    password=con_config['password_oracle'],
                    dsn=oracledb.makedsn(con_config['host_oracle'], con_config['port_oracle'],
                                         con_config['service_name_oracle'])
                )
                print('Соединение с ', con_config['host_oracle'], 'установлено успешно!\n')

            elif database_type == 'postgres_out':
                print('Установка соединения с ', con_config['host_postgres_out'], '....\n')

                con = psycopg2.connect(
                    user=con_config['user_postgres_out'],
                    password=con_config['password_postgres_out'],
                    host=con_config['host_postgres_out'],
                    port=con_config['port_postgres_out'],
                    database=con_config['database_postgres_out']
                )
                print('Соединение с ', con_config['host_postgres_out'], 'установлено успешно!\n')

            else:
                print("Неизвестный тип базы данных.")
                return None

            return con

        except pymysql.DatabaseError as err_my:
            raise AirflowException(f"MySQL Database Error: {err_my}")
            quit()
        except oracledb.DatabaseError as err_or:
            raise AirflowException(f"Oracle Database Error: {err_or}")
            quit()
        except psycopg2.DatabaseError as err_po:
            raise AirflowException(f"PostgreSQL Database Error: {err_po}")
            quit()
        except Exception as er_gen:
            raise AirflowException(f"General Error: {str(er_gen)}")
            quit()

    def upload_data(self, index):
        con = self.connect_to_out_source(self.database_type)

        try:
            print('Подготовка данных для загрузки в таблицу  ', self.config[self.keys[index]]['postgres_table'],
                  '....\n')

            cur = con.cursor()
            cur.execute(self.config[f'{self.keys[index]}'][f'{self.database_type}_table'])  # Not sure for now
            rows = cur.fetchall()
            df = DataFrame(rows, columns=self.config[f'{self.keys[index]}']['select_columns'])

            #print(df.shape)

            expected_types, date_formats = extract_expected_columns_and_date_formats(self.config, self.keys, index)
            print(expected_types)

            failed_rows, error_messages = validate_dataframe(df, expected_types, date_formats)
#            print(failed_rows)

            failed_rows_df = pd.DataFrame(failed_rows, columns=df.columns)

            df.drop(failed_rows_df.index, inplace=True)

#            print(df)
            print('Данные для загрузки в таблицу  ', self.config[self.keys[index]]['postgres_table'],
                  ' успешно подготовлены\n')

        except pymysql.DatabaseError as err_my:
            raise AirflowException(f"MySQL Database Error: {err_my}")
            quit()
        except oracledb.DatabaseError as err_or:
            raise AirflowException(f"Oracle Database Error: {err_or}")
            quit()
        except psycopg2.DatabaseError as err_po:
            raise AirflowException(f"PostgreSQL Database Error: {err_po}")
            quit()
        except Exception as er_gen:
            raise AirflowException(f"General Error: {str(er_gen)}")
            quit()

        try:
            print('Установка соединения с ', con_config['host_postgres'], '....\n')

            cnxn = create_engine(con_config['cnxn_eng'])

            print('Соединение с ', con_config['host_postgres'], 'установлено успешно!\n')

        except psycopg2.DatabaseError as postg_er:
            raise AirflowException(f"PostgreSQL Database Error: {postg_er}")
            quit()

        except Exception as f:
            raise AirflowException(f"General Error: {str(f)}")
            quit()

        try:
            print(f'Загрузка данных в таблицу ', self.config[self.keys[index]]['postgres_table'], ' начата\n')

            df.to_sql(
                self.config[f'{self.keys[index]}']['postgres_table'],
                con=cnxn,
                schema='oplati_stage_level',
                if_exists='append',
                index=False
            )

            write_errors_to_database(
                df=df,
                failed_rows=failed_rows,
                error_messages=error_messages,
                process_name=f'{self.database_type}_upload',
                connection=cnxn,
                config=self.config,
                keys=self.keys,
                index=index,
                sql_statement_key=f'{self.database_type}_table'
            )

            print(f'Загрузка данных в таблицу ', self.config[self.keys[index]]['postgres_table'],
                  ' окончена успешно\n')

        except Exception as d:
            print(f'Данные не загружены в таблицу', self.config[self.keys[index]]['postgres_table'], ' ...\n')

            raise AirflowException('Ошибка: ', d)
            quit()

        con.commit()
        cur.close()
        con.close()
