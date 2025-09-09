from class_task_groups.configs import con_config, check_config

from pandas import DataFrame
from sqlalchemy import create_engine

import json
import oracledb
import psycopg2
import requests
#import time

check_keys = list(check_config.keys())


# ----Check STG Tables----####################################


def check_data(index, *args):
    try:

        connection = psycopg2.connect(user=con_config['user_postgres'],
                                      password=con_config['password_postgres'],
                                      host=con_config['host_postgres'],
                                      port=con_config['port_postgres'],
                                      database=con_config['database_postgres'])

    except psycopg2.DatabaseError as poer:
        print('Ошибка: ', poer)
        quit()

    retry_limit = 3
    retry_count = 0

    while retry_count < retry_limit:
        try:
            print('Check data in ', check_config[f'{check_keys[index]}']['postgres_table'])
            cur = connection.cursor()
            cur.execute(check_config[f'{check_keys[index]}']['postgres_sql'])
            rows = cur.fetchall()
            df_5 = DataFrame(rows)

            #print(df_5)

            if not df_5.empty:
                True
                print('There is data in the table')
                break

            else:

                retry_count += 1

                print(f"Retrying number : {retry_count}")
                print(f"Retrying in 120 seconds...")
                time.sleep(120)

                try:
                    print('Установка соединения с ', con_config['host_oracle'], '....\n')

                    con = oracledb.connect(
                        user=con_config['username_oracle'],
                        password=con_config['password_oracle'],
                        dsn=oracledb.makedsn(con_config['host_oracle'], con_config['port_oracle'],
                                             con_config['service_name_oracle'])
                    )
                    print('Соединение с ', con_config['host_oracle'], 'установлено успешно!\n')

                except oracledb.DatabaseError as er_con:
                    print('There is an error in the Oracle database:', er_con)

                try:

                    print('Подготовка данных для загрузки в таблицу  ',
                          check_config[check_keys[index]]['postgres_table'], '....\n')

                    cur = con.cursor()
                    cur.execute(check_config[f'{check_keys[index]}']['oracle_table'])
                    rows = cur.fetchall()

                    df_3 = DataFrame(rows, columns=check_config[f'{check_keys[index]}']['select_columns'])
                    #print(df_3)
                    print('Данные для загрузки в таблицу  ', check_config[check_keys[index]]['postgres_table'],
                          ' успешно подготовлены\n')

                    if df_3.empty:
                        print('DataFrame is empty!')

                    con.commit()
                    cur.close()
                    con.close()

                except Exception as er:
                    print('Ошибка:' + str(er))
                    quit()

                try:
                    print('Установка соединения с ', con_config['host_postgres'], '....\n')
                    cnxn = create_engine(con_config['cnxn_eng'])
                    print('Соединение с ', con_config['host_postgres'], 'установлено успешно!\n')

                except psycopg2.DatabaseError as postg_er:
                    print('Ошибка: ', postg_er)
                    quit()

                try:
                    print(f'Загрузка данных в таблицу ', check_config[check_keys[index]]['postgres_table'], ' начата\n')

                    df_3.to_sql(check_config[f'{check_keys[index]}']['postgres_table'], con=cnxn,
                                schema='oplati_stage_level',
                                if_exists='append', index=False)

                    print(f'Загрузка данных в таблицу ', check_config[check_keys[index]]['postgres_table'],
                          ' окончена успешно\n')

                except psycopg2.DatabaseError as poer:
                    print('Ошибка: ', poer)
                    quit()

                except Exception as d:
                    print(f'Данные не загружены в таблицу', check_config[check_keys[index]]['postgres_table'], '...\n')
                    print('Error: ', d)
                    quit()

            connection.commit()
            cur.close()
            connection.close()

        except Exception as f:
            print('Ошибка: ', f)
            quit()


###############################----Function for API---###########################################


def pos_coordinates(*op_args, **op_kwargs):
    connection = psycopg2.connect(user=con_config['user_postgres'],
                                  password=con_config['password_postgres'],
                                  host=con_config['host_postgres'],
                                  port=con_config['port_postgres'],
                                  database=con_config['database_postgres'])

    select_users = ("select pos_id,org_id,latitude,longitude from oplati_stage_level.stg_merchant_pos smp"
                    " where pos_id not in (select pos_id from oplati_statistic.spr_pos_area amp );")

    cursor = connection.cursor()
    cursor.execute(select_users)
    users = cursor.fetchall()

    df = DataFrame(users, columns=['pos_id', 'org_id', 'latitude', 'longitude'])
    # export_csv = df.to_csv(r'example6.csv', index=None, header=True)
    df = df[["pos_id", "org_id", "latitude", "longitude"]].to_dict("records")

    #print(df)

    apikey = '888888888888888888888888888888888888888888888' # ключ 

    url = 'http://geocode-maps.yandex.ru/1.x/'
    dict = []

    for string in df:
        coord = str(string['longitude']) + ',' + str(string['latitude'])
        post_params = {"geocode": coord, "apikey": apikey, "format": 'json'}
        response_get = requests.get(url, post_params, verify=False)

        data = response_get.json()
        address = data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['name']
        try:
            city = data['response']['GeoObjectCollection']['featureMember'][0][
                'GeoObject']['metaDataProperty']['GeocoderMetaData']['AddressDetails'][
                'Country']['AdministrativeArea']['SubAdministrativeArea']['Locality']['LocalityName']
        except Exception:
            try:
                city = data['response']['GeoObjectCollection']['featureMember'][0][
                    'GeoObject']['metaDataProperty']['GeocoderMetaData'][
                    'AddressDetails']['Country']['AdministrativeArea']['Locality']['LocalityName']
            except Exception:
                try:
                    city = data['response']['GeoObjectCollection']['featureMember'][0][
                        'GeoObject']['metaDataProperty']['GeocoderMetaData']['AddressDetails'][
                        'Country']['AdministrativeArea']['SubAdministrativeArea']['SubAdministrativeAreaName']
                except Exception:
                    city = 'NULL'
        try:
            area = data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['metaDataProperty'][
                'GeocoderMetaData']['AddressDetails']['Country']['AdministrativeArea']['SubAdministrativeArea'][
                'SubAdministrativeAreaName']
        except Exception:
            try:
                area = data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['metaDataProperty'][
                    'GeocoderMetaData']['AddressDetails']['Country']['AdministrativeArea']['Locality']['LocalityName']
            except Exception:
                area = 'NULL'
        try:
            region = data['response']['GeoObjectCollection']['featureMember'][0]['GeoObject']['metaDataProperty'][
                'GeocoderMetaData'][
                'AddressDetails']['Country']['AdministrativeArea']['AdministrativeAreaName']
        except Exception:
            region = 'NULL'
        result = {"pos_id": string['pos_id'], "org_id": string['org_id'], "latitude": string['latitude']
            , "longitude": string['longitude'], "address": address, "city": city, "area": area, "region": region}
        dict.append(result)

        print(dict)

    cursor.execute(
        f"INSERT INTO oplati_stage_level.stg_json (data_json) values ('{f'{json.dumps(dict, ensure_ascii=True)}'}')")
    connection.commit()

    connection.commit()
    cursor.close()
    connection.close()
