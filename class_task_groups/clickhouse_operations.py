from class_task_groups.configs import db_transfer_config, clickhouse_common_3, clickhouse_common_2, clickhouse_common_1 
from class_task_groups.configs import con_config
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
from pandas import DataFrame
import clickhouse_connect
import psycopg2

PREV_DAY = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
THE_DAY_BEFORE_PREV_DAY = (datetime.now().date() - timedelta(days=2)).strftime("%Y-%m-%d")

db_transfer_config_keys = list(db_transfer_config.keys())

clickhouse_common_3_keys = list(clickhouse_common_3.keys())
clickhouse_common_2_keys = list(clickhouse_common_2.keys())
clickhouse_common_1_keys = list(clickhouse_common_1.keys())

# ----Function for data transfer from Postgres to Clickhouse---###########################

def clickhouse_upload(cl_index, *args):
    try:
        print('Установка соединения с ', con_config['host_postgres'], '....\n')

        con = psycopg2.connect(
            user=con_config['user_postgres'],
            password=con_config['password_postgres'],
            host=con_config['host_postgres'],
            port=con_config['port_postgres'],
            database=con_config['database_postgres']
        )
        print('Соединение с ', con_config['host_postgres'], 'установлено успешно!\n')

    except psycopg2.DatabaseError as er_con:
        raise AirflowException('Ошибка:', er_con)
        quit()
    except Exception as er_gen:
        raise AirflowException('Ошибка:' + str(er_gen))
        quit()

    keys = db_transfer_config[db_transfer_config_keys[cl_index]].keys()

    try:

        print('Подготовка данных для выгрузки из таблицы  ',
              db_transfer_config[db_transfer_config_keys[cl_index]]['postgres_table'], '....\n')

        cur = con.cursor()

        columns_list = db_transfer_config[db_transfer_config_keys[cl_index]]['clickhouse_columns']

        postgres_schema = db_transfer_config[db_transfer_config_keys[cl_index]]['postgres_schema']
        postgres_table = db_transfer_config[db_transfer_config_keys[cl_index]]['postgres_table']

        # keys = db_transfer_config[db_transfer_config_keys[cl_index]].keys()

        if 'datetime_field' in keys:
            datetime_field = db_transfer_config[db_transfer_config_keys[cl_index]]['datetime_field']

            query = (f"SELECT {columns_list} FROM {postgres_schema}.{postgres_table}"
                     f" WHERE date_trunc('day',{datetime_field}) = '{PREV_DAY}'")

        elif 'postgres_query' in keys:
            postgres_query = db_transfer_config[db_transfer_config_keys[cl_index]]['postgres_query']

            query = postgres_query

        else:

            query = f"SELECT {columns_list} FROM {postgres_schema}.{postgres_table}"

        cur.execute(query)
        rows = cur.fetchall()

        columns_list = columns_list.split(", ")

        df_5 = DataFrame(rows, columns=columns_list)

        cur.close()
        con.close()

    except psycopg2.DatabaseError as er:
        raise AirflowException('There is an error in the Postgres database:', er)
        quit()
    except Exception as er:
        raise AirflowException('Error:' + str(er))
        quit()

    try:

        client = clickhouse_connect.get_client(
            host=con_config['host_clickhouse'],
            port=con_config['port_clickhouse'],
            username=con_config['user_clickhouse'],
            password=con_config['password_clickhouse'])

        clickhouse_table = db_transfer_config[db_transfer_config_keys[cl_index]]['clickhouse_table']
        clickhouse_schema = db_transfer_config[db_transfer_config_keys[cl_index]]['clickhouse_schema']

        if 'truncate_clickhouse' in keys:

            client.command(f"truncate table {clickhouse_schema}.{clickhouse_table}")
            print(f'Table {clickhouse_table} is truncated')

            client.insert(f"{clickhouse_schema}.{clickhouse_table}", df_5)
            print(f'Таблица загружена  {clickhouse_table}  ')

        else:

            client.insert(f"{clickhouse_schema}.{clickhouse_table}", df_5)
            print(f'Таблица загружена  {clickhouse_table}  ')

    except Exception as d:
        raise AirflowException('Error: ', d)
        quit()

# ----Function for executing queries in Clickhouse---###########################

def clickhouse_query_execution_3(cl_query_index, *args):
    try:
        print('Подготовка к выполнению запроса')

        clickhouse_query = clickhouse_common_3[clickhouse_common_3_keys[cl_query_index]]['clickhouse_query']

        keys = clickhouse_common_3[clickhouse_common_3_keys[cl_query_index]].keys()

        client = clickhouse_connect.get_client(
            host=con_config['host_clickhouse'],
            port=con_config['port_clickhouse'],
            username=con_config['user_clickhouse'],
            password=con_config['password_clickhouse'])

        print("Test connection")

        # df_6 = client.command(clickhouse_query)
        #
        # print('Запрос ', clickhouse_query, ' успешно выполнен.\n')

        if 'overwrite_period' in keys:

            clickhouse_schema = clickhouse_common_3[clickhouse_common_3_keys[cl_query_index]]['clickhouse_schema']
            clickhouse_table = clickhouse_common_3[clickhouse_common_3_keys[cl_query_index]]['clickhouse_table']
            datetime_field = clickhouse_common_3[clickhouse_common_3_keys[cl_query_index]]['datetime_field']
            recount_period = clickhouse_common_3[clickhouse_common_3_keys[cl_query_index]]['recount_period']

            print("Подготовка к перезаписи значений для текущего периода")

            if recount_period == 'day':
                client.command(f"truncate table {clickhouse_schema}.{clickhouse_table}")

            else:
                if 'remove_current' in keys:
                    client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                                   f" where date_trunc('{recount_period}', {datetime_field}) ="
                                   f" date_trunc('{recount_period}', today());")
                else:
                    client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                                   f" where date_trunc('{recount_period}', {datetime_field}) ="
                                   f" date_trunc('{recount_period}', today() - 1);")

                # client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                #                f" where date_trunc('{recount_period}', {datetime_field}) ="
                #                f" date_trunc('{recount_period}', today() - 1);")

            print("Значения за текущий период удалены")

            df_6 = client.command(clickhouse_query)

        else:

            df_6 = client.command(clickhouse_query)
            #print(df_6)

        print('Запрос успешно выполнен.\n')

    except Exception as query_err:
        raise AirflowException('Error: ', query_err)
        quit()

# ----Function for executing queries in Clickhouse---###########################

def clickhouse_query_execution_1(cl_query_index, *args):
    try:
        print('Подготовка к выполнению запроса')

        clickhouse_query = clickhouse_common_1[clickhouse_common_1_keys[cl_query_index]]['clickhouse_query']

        keys = clickhouse_common_1[clickhouse_common_1_keys[cl_query_index]].keys()

        client = clickhouse_connect.get_client(
            host=con_config['host_clickhouse'],
            port=con_config['port_clickhouse'],
            username=con_config['user_clickhouse'],
            password=con_config['password_clickhouse'])

        print("Test connection")

        # df_6 = client.command(clickhouse_query)
        #
        # print('Запрос ', clickhouse_query, ' успешно выполнен.\n')

        if 'overwrite_period' in keys:

            clickhouse_schema = clickhouse_common_1[clickhouse_common_1_keys[cl_query_index]]['clickhouse_schema']
            clickhouse_table = clickhouse_common_1[clickhouse_common_1_keys[cl_query_index]]['clickhouse_table']
            datetime_field = clickhouse_common_1[clickhouse_common_1_keys[cl_query_index]]['datetime_field']
            recount_period = clickhouse_common_1[clickhouse_common_1_keys[cl_query_index]]['recount_period']

            print("Подготовка к перезаписи значений для текущего периода")

            if recount_period == 'day':
                client.command(f"truncate table {clickhouse_schema}.{clickhouse_table}")

            else:
                if 'remove_current' in keys:
                    client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                                   f" where date_trunc('{recount_period}', {datetime_field}) ="
                                   f" date_trunc('{recount_period}', today());")
                else:
                    client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                                   f" where date_trunc('{recount_period}', {datetime_field}) ="
                                   f" date_trunc('{recount_period}', today() - 1);")

                # client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                #                f" where date_trunc('{recount_period}', {datetime_field}) ="
                #                f" date_trunc('{recount_period}', today() - 1);")

            print("Значения за текущий период удалены")

            df_6 = client.command(clickhouse_query)
            print(df_6)

        else:

            df_6 = client.command(clickhouse_query)
            print(df_6)

            #print(df_6)

        print('Запрос успешно выполнен.\n')

    except Exception as query_err:
        raise AirflowException('Error: ', query_err)
        quit()
        
# ----Function for executing queries in Clickhouse---###########################

def clickhouse_query_execution_2(cl_query_index, *args):
    try:
        print('Подготовка к выполнению запроса')

        clickhouse_query = clickhouse_common_2[clickhouse_common_2_keys[cl_query_index]]['clickhouse_query']

        keys = clickhouse_common_2[clickhouse_common_2_keys[cl_query_index]].keys()

        client = clickhouse_connect.get_client(
            host=con_config['host_clickhouse'],
            port=con_config['port_clickhouse'],
            username=con_config['user_clickhouse'],
            password=con_config['password_clickhouse'])

        print("Test connection")

        # df_6 = client.command(clickhouse_query)
        #
        # print('Запрос ', clickhouse_query, ' успешно выполнен.\n')

        if 'overwrite_period' in keys:

            clickhouse_schema = clickhouse_common_2[clickhouse_common_2_keys[cl_query_index]]['clickhouse_schema']
            clickhouse_table = clickhouse_common_2[clickhouse_common_2_keys[cl_query_index]]['clickhouse_table']
            datetime_field = clickhouse_common_2[clickhouse_common_2_keys[cl_query_index]]['datetime_field']
            recount_period = clickhouse_common_2[clickhouse_common_2_keys[cl_query_index]]['recount_period']

            print("Подготовка к перезаписи значений для текущего периода")

            if recount_period == 'day':
                client.command(f"truncate table {clickhouse_schema}.{clickhouse_table}")

            else:
                if 'remove_current' in keys:
                    client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                                   f" where date_trunc('{recount_period}', {datetime_field}) ="
                                   f" date_trunc('{recount_period}', today());")
                else:
                    client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                                   f" where date_trunc('{recount_period}', {datetime_field}) ="
                                   f" date_trunc('{recount_period}', today() - 1);")

                # client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                #                f" where date_trunc('{recount_period}', {datetime_field}) ="
                #                f" date_trunc('{recount_period}', today() - 1);")

            print("Значения за текущий период удалены")

            df_6 = client.command(clickhouse_query)

        else:

            df_6 = client.command(clickhouse_query)
            #print(df_6)

        print('Запрос успешно выполнен.\n')

    except Exception as query_err:
        raise AirflowException('Error: ', query_err)
        quit()


class ClickhouseQueryExecutor:
    def __init__(self, config, keys):
        self.config = config
        self.keys = keys

    def execute_query(self, cl_query_index):
        try:
            print('Подготовка к выполнению запроса')

            clickhouse_query = self.config[self.keys[cl_query_index]]['clickhouse_query']

            keys = self.config[self.keys[cl_query_index]].keys()

            client = clickhouse_connect.get_client(
                host=con_config['host_clickhouse'],
                port=con_config['port_clickhouse'],
                username=con_config['user_clickhouse'],
                password=con_config['password_clickhouse'])

            print("Test connection")

            # df_6 = client.command(clickhouse_query)
            #
            # print('Запрос ', clickhouse_query, ' успешно выполнен.\n')

            if 'overwrite_period' in keys:

                clickhouse_schema = self.config[self.keys[cl_query_index]]['clickhouse_schema']
                clickhouse_table = self.config[self.keys[cl_query_index]]['clickhouse_table']
                datetime_field = self.config[self.keys[cl_query_index]]['datetime_field']
                recount_period = self.config[self.keys[cl_query_index]]['recount_period']

                print("Подготовка к перезаписи значений для текущего периода")

                if recount_period == 'day':
                    client.command(f"truncate table {clickhouse_schema}.{clickhouse_table}")

                else:
                    if 'remove_current' in keys:
                        client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                                       f" where date_trunc('{recount_period}', {datetime_field}) ="
                                       f" date_trunc('{recount_period}', today());")
                    else:
                        client.command(f"delete from {clickhouse_schema}.{clickhouse_table}"
                                       f" where date_trunc('{recount_period}', {datetime_field}) ="
                                       f" date_trunc('{recount_period}', today() - 1);")

                print("Значения за текущий период удалены")

                df_6 = client.command(clickhouse_query)

            else:

                df_6 = client.command(clickhouse_query)
                # print(df_6)

            print('Запрос успешно выполнен.\n')

        except Exception as query_err:
            raise AirflowException('Error: ', query_err)
            quit()
