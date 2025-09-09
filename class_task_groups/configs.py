import yaml

# ----Configs---###########################################


# Загрузка файла .yaml c запросами на выгрузку данных из Oracle
try:
    with open('/srv/airflow/data/dags/configs/oracle_requests.yaml') as ora:
        ora_config = yaml.load(ora, Loader=yaml.FullLoader)

        print('Конфигурационный файл c запросами для Oracle подключен')


except Exception as eOra:

    print('Ошибка файл c запросами для Oracle: ', eOra)
    quit()

# Загрузка файла .yaml c запросами на выгрузку данных из Bitrix

try:
    with open('/srv/airflow/data/dags/configs/mysql_requests.yaml') as msql:
        bitrix_config = yaml.load(msql, Loader=yaml.FullLoader)

        print('Конфигурационный файл c запросами для MySql подключен')


except Exception as eMsql:

    print('Ошибка файла c запросами для MySql: ', eMsql)
    quit()

# Загрузка файла .yaml c запросами на выгрузку данных из Postgres

try:
    with open('/srv/airflow/data/dags/configs/postgres_requests.yaml') as post:
        postgresOut_config = yaml.load(post, Loader=yaml.FullLoader)

        print('Конфигурационный файл c запросами для PostgreSql подключен')


except Exception as ePost:

    print('Ошибка файла c запросами для PostgreSql: ', ePost)
    quit()

# Загрузка файла .yaml c процедурами обработки данных

try:
    with open('/srv/airflow/data/dags/configs/procedures_config.yaml') as proc:
        procedures_config = yaml.load(proc, Loader=yaml.FullLoader)

        print('Конфигурационный файл c процедурами')


except Exception as eProc:

    print('Ошибка файла c процедурами: ', eProc)
    quit()

# Загрузка файла c параметрами подключений к сторонним базам

try:
    with open('/srv/airflow/data/dags/configs/conection_config.yaml') as fh:
        con_config = yaml.load(fh, Loader=yaml.FullLoader)

        print('Конфигурационный файл c параметрами подключения подключен')


except Exception as er:

    print('Ошибка файла с параметрами подключений:', er)
    quit()

# Загрузка файла .yaml c параметрами рассылки в Telegram

try:
    with open('/srv/airflow/data/dags/configs/telegram_message/telegram_stats_requests.yaml') as fc:
        telegramStat_configs = yaml.load(fc, Loader=yaml.FullLoader)

        print('Конфигурационный файл c параметрами подключения для рассылки статистики подключен')


except Exception as tr:

    print('Ошибка файла с параметрами рассылки в Telegram:', tr)
    quit()

# Загрузка файла .yaml c параметрами проверки stg таблиц

try:
    with open('/srv/airflow/data/dags/configs/check_stg_requests.yaml') as ora:
        check_config = yaml.load(ora, Loader=yaml.FullLoader)

        print('Конфигурационный файл c запросами для Oracle подключен (check account balance)')

except Exception as eOra:

    print('Ошибка файл c запросами для Oracle (check account balance): ', eOra)
    quit()

# Загрузка файла .yaml c параметрами рассылки ошибок в Telegram

try:
    with open('/srv/airflow/data/dags/configs/telegram_message/telegram_stats_errors.yaml') as fc:
        telegram_error_log_message_configs = yaml.load(fc, Loader=yaml.FullLoader)

        print('Конфигурационный файл c параметрами запроса для таблицы ошибок подключен')

except Exception as tr:

    print('Ошибка файла с запросом для таблицы ошибок:', tr)
    quit()

# Загрузка файла .yaml c параметрами таблиц для загрузки в clickhouse

try:
    with open('/srv/airflow/data/dags/configs/postgres_clickhouse_transfer.yaml') as pct:
        db_transfer_config = yaml.load(pct, Loader=yaml.FullLoader)

        print(
            'Конфигурационный файл c названиями полей и таблиц в Clickhouse подключен')

except Exception as pct_error:

    print('Ошибка файла с названиями полей и таблиц Clickhouse: ', pct_error)
    quit()
    
try:
    with open('/srv/airflow/data/dags/configs/dashboard_configs/clickhouse_queries_part_3.yaml') as comm:
        clickhouse_common_3 = yaml.load(comm, Loader=yaml.FullLoader)

        print(
            'Конфигурационный файл c названиями полей и таблиц агрегированных значений в Clickhouse подключен')

except Exception as comm_error:

    print('Ошибка файла с названиями полей и таблиц агрегированных значений в Clickhouse: ', comm_error)
    quit()


try:
    with open('/srv/airflow/data/dags/configs/dashboard_configs/uncatched_tables_queries.yaml') as coi:
        clickhouse_uncatched_config = yaml.load(coi, Loader=yaml.FullLoader)

        print(
            'Конфигурационный файл c названиями полей и таблиц агрегированных значений в Clickhouse подключен')

except Exception as comm_error:

    print('Ошибка файла с названиями полей и таблиц агрегированных значений в Clickhouse: ', comm_error)
    quit()



try:
    with open('/srv/airflow/data/dags/configs/dashboard_configs/clickhouse_queries_part_2.yaml') as comm2:
        clickhouse_common_2 = yaml.load(comm2, Loader=yaml.FullLoader)

        print(
            'Конфигурационный файл c названиями полей и таблиц агрегированных значений в Clickhouse подключен')

except Exception as comm_error2:

    print('Ошибка файла с названиями полей и таблиц агрегированных значений в Clickhouse: ', comm_error2)
    quit()

try:
    with open('/srv/airflow/data/dags/configs/dashboard_configs/clickhouse_queries_part_1.yaml') as comm1:
        clickhouse_common_1 = yaml.load(comm1, Loader=yaml.FullLoader)

        print(
            'Конфигурационный файл c названиями полей и таблиц агрегированных значений в Clickhouse подключен')

except Exception as comm_error1:

    print('Ошибка файла с названиями полей и таблиц агрегированных значений в Clickhouse: ', comm_error1)
    quit()

# Загрузка файла .yaml c параметрами срезов таблиц для загрузки в clickhouse (сегменты)

try:
    with open('/srv/airflow/data/dags/configs/dashboard_configs/slice_tables.yaml') as pct:
        slice_tables_config = yaml.load(pct, Loader=yaml.FullLoader)

        print('Конфигурационный файл cо срезами таблиц в Clickhouse подключен')

except Exception as stc_error:

    print('Ошибка файла cо срезами таблиц в Clickhouse: ', stc_error)
    quit()

