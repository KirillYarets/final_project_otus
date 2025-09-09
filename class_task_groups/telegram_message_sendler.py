from class_task_groups.configs import con_config
from class_task_groups.configs import telegram_error_log_message_configs, telegramStat_configs

from datetime import datetime

import numpy as np
import requests
import pandas as pd
import psycopg2

telegramStats_keys = ["111111111", "2222222222", "33333333333"]
telegramStats_range_keys = range(len(telegramStats_keys))
telegramStats_range_index = []

telegramStats_keys_bib = ["11111111111","222222222", "333333333333", "44444444444", "5555555555"]
telegramStats_range_keys_bib = range(len(telegramStats_keys_bib))
telegramStats_range_bib_index = []



###############################----Telegram messages---###########################################


token_tel = "1111111111111111111111111111111111111"
url_t = "https://api.telegram.org/bot"
chat_id = ["813496977", "315838881"]
current_date = datetime.now().date()


def telegram_error_log_message():
    connection = psycopg2.connect(user=con_config['user_postgres'],
                                  password=con_config['password_postgres'],
                                  host=con_config['host_postgres'],
                                  port=con_config['port_postgres'],
                                  database=con_config['database_postgres'])

    print("Database opened successfully")

    cur = connection.cursor()

    error_log_message = telegram_error_log_message_configs['error_log_message']

    df_error_log_message = pd.read_sql(error_log_message, connection)

    error_log_message_new_rows = df_error_log_message.at[0, 'count']

    for ch_id in chat_id:
        try:
            token = token_tel
            url = url_t
            channel_id = ch_id
            url += token
            method = url + "/sendMessage"

            if error_log_message_new_rows:
                r = requests.post(method, data={
                    "chat_id": channel_id,
                    "text": f"Количество новых ошибок при валидации: {error_log_message_new_rows}!"
                })
        except Exception as tfr:
            print(tfr)


def telegram_good_message():
    for ch_id in chat_id:
        try:
            token = token_tel
            url = url_t
            channel_id = ch_id
            url += token
            method = url + "/sendMessage"

            r = requests.post(method, data={
                "chat_id": channel_id,
                "text": f"Загрузка данных '{current_date}' отработала успешно !"
            })
        except Exception as tfr:
            print(tfr)


def telegram_stats(t_index):
    connection = psycopg2.connect(user=con_config['user_postgres'],
                                  password=con_config['password_postgres'],
                                  host=con_config['host_postgres'],
                                  port=con_config['port_postgres'],
                                  database=con_config['database_postgres'])

    print("Database opened successfully")

    cur = connection.cursor()

    string_wallet = telegramStat_configs['wallet_data']
    string_bal = telegramStat_configs['bal_data']
    string_telega = telegramStat_configs['telega_data']
    string_cashbox = telegramStat_configs['cashbox_data']
    string_indicators = telegramStat_configs['indicators_data']
    string_azs = telegramStat_configs['azs_data']
    string_pos = telegramStat_configs['pos_data']
    string_erip = telegramStat_configs['erip_data']
    string_vcard = telegramStat_configs['vcard_data']
    string_new_wallet = telegramStat_configs['new_wallet_data']

    df_wal = pd.read_sql(string_wallet, connection)
    df_bal = pd.read_sql(string_bal, connection)
    df_teleg = pd.read_sql(string_telega, connection)
    df_cash = pd.read_sql(string_cashbox, connection)
    df_ind = pd.read_sql(string_indicators, connection)
    df_azs = pd.read_sql(string_azs, connection)
    df_pos = pd.read_sql(string_pos, connection)
    df_erip = pd.read_sql(string_erip, connection)
    df_vcard = pd.read_sql(string_vcard, connection)
    df_n_wal = pd.read_sql(string_new_wallet, connection)

    token_tel = "222222222222222222222222222222222222222222222222"
    url_t = "https://api.telegram.org/bot"

    token = token_tel
    url = url_t
    channel_id = telegramStats_keys[t_index]
    url += token
    method = url + "/sendMessage"

    requests.post(method, data={
        "chat_id": channel_id,
        "text": f"\n########_Кошельки_########\
                        \n\
                        \nTotal(осн.)\t\t\t\tNew(осн.)\t\t\t\tBal(все)\t\t\t\n{df_wal.values[6, 2]},\t\t\t\t\t\t\t\t\t+{df_wal.values[6, 4]},\t\t\t\t\t\t\t\t\t\t\t\t\t{np.round((df_wal.values[6, 5] + df_wal.values[5, 5] + df_wal.values[4, 5] + df_wal.values[3, 5]), decimals=2)} \
                        \n\
                        \n{(df_wal.values[6, 1])}: \nTotal\t\t\t\t\t\t\t\tNew\t\t\t\t\t\t\tBal\t\t\t\t\t\t\n{df_wal.values[6, 2]},   +{df_wal.values[6, 4]},   \t{df_wal.values[6, 5]} \
                        \n\
                        \n{df_wal.values[5, 1]}: \nTotal\t\t\t\t\t\tNew\t\t\t\t\tBal\t\t\t\t\t\t\n{df_wal.values[5, 2]},\t\t\t+{df_wal.values[5, 4]},\t\t\t\t\t{df_wal.values[5, 5]}\
                        \n\
                        \n{df_wal.values[3, 1]}: \nTotal\t\tNew\t\t\t\tBal\n{df_wal.values[3, 2]},\t\t\t+{df_wal.values[3, 4]},\t\t\t\t\t{df_wal.values[3, 5]}\
                        \n\
                        \n{df_wal.values[8, 1]}: \nTotal\t\t\t\t\t\tNew\t\t\t\t\t\tBal\n{df_wal.values[8, 2]},\t\t\t\t\t\t+{df_wal.values[8, 4]},\t\t\t\t\t{df_wal.values[8, 5]}\
                        \n\
                        \n{df_wal.values[9, 1]}: \nTotal\t\t\t\t\t\tNew\t\t\t\t\t\tBal\n{df_wal.values[9, 2]},\t\t\t\t\t+{df_wal.values[9, 4]},\t\t\t\t\t\t{df_wal.values[9, 5]}\
                        \n\
                        \n{df_wal.values[10, 1]}: \nTotal\t\t\t\t\tNew\t\t\tBal\n{df_wal.values[10, 2]},\t\t\t\t\t\t\t\t\t\t\t+{df_wal.values[10, 4]},\t\t\t\t\t\t{df_wal.values[10, 5]}\
                        \n\
                        \nВсего кошельков: \nNew_wallet \t\t\t\t\tTotal_wallet  \n{df_n_wal.values[0, 0]},\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t{df_n_wal.values[0, 1]} \
                        \n\
                        \n#######_Остатки_#########\
                        \n\
                        \nОстатки на кошельках физ.лиц: \nSum \n{df_bal.values[0, 0]}\
                        \n\
                        \nОстатки на кошельках юр.лиц: \nSum \n{df_teleg.values[0, 0]}\
                        \n\
                        \n#######_Магазины_#######\
                        \n\
                        \nВсего по магазинам: \nOper  \t\t\t\tSum  \t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_ind.values[0, 1]},  \t\t {df_ind.values[0, 2]},   \t\t{df_teleg.values[0, 1]}\
                        \n\
                        \n{df_cash.values[0, 1]}: \nOper  \t\tSum  \t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_cash.values[0, 3]},  \t\t\t{df_cash.values[0, 4]}, \t\t\t\t{df_teleg.values[0, 2]}\
                        \n\
                        \n{df_cash.values[1, 1]}: \nOper  \t\tSum  \t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_cash.values[1, 3]},  \t{df_cash.values[1, 4]}, \t\t\t{df_teleg.values[0, 3]}\
                        \n\
                        \n{df_cash.values[2, 1]}: \nOper  \t\tSum  \t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_cash.values[2, 3]},  \t\t\t{df_cash.values[2, 4]}, \t\t\t{df_teleg.values[0, 4]}\
                        \n\
                        \n{df_cash.values[3, 1]}: \nOper  \t\tSum  \t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_cash.values[3, 3]},  \t\t\t{df_cash.values[3, 4]}, \t\t\t\t\t\t{df_teleg.values[0, 6]}\
                        \n\
                        \n{df_cash.values[4, 1]}: \nOper  \t\tSum  \t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_cash.values[4, 3]},  \t\t\t{df_cash.values[4, 4]}, \t\t\t\t\t\t{df_teleg.values[0, 5]}\
                        \n\
                        \n#######_АЗС(заправки)_#######\
                        \n\
                        \nАЗС(Всего):  \nOper \t\t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_azs.values[0, 0]}, \t\t\t\t{df_azs.values[0, 1]},\t\t\t\t\t\t{df_azs.values[0, 2]}\
                        \n\
                        \nАЗС(Белоруснефть):  \nOper \t\t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_azs.values[0, 6]}, \t\t\t\t\t\t{df_azs.values[0, 7]},\t\t\t\t\t\t{df_azs.values[0, 8]}\
                        \n\
                        \nАЗС(А-100):  \nOper \t\t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_azs.values[0, 3]}, \t\t\t\t\t\t{df_azs.values[0, 4]},\t\t\t\t\t\t\t\t{df_azs.values[0, 5]}\
                        \n\
                        \nАЗС(United Company):  \nOper \t\t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_azs.values[0, 9]}, \t\t\t\t\t\t\t\t{df_azs.values[0, 10]},\t\t\t\t\t\t\t\t{df_azs.values[0, 11]}\
                        \n\
                        \n#######_Платежи_#######\
                        \n\
                        \nPos-платежи:  \nOper \t\t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_pos.values[0, 1]}, \t\t{df_pos.values[0, 2]}, \t\t\t{df_pos.values[0, 3]}\
                        \n\
                        \nPos-платежи (продажи товаров и услуг):  \nOper \t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_pos.values[0, 4]}, \t\t{df_pos.values[0, 5]}, \t\t\t{df_pos.values[0, 6]}\
                        \n\
                        \nPos-платежи (перевозка пассажиров):  \nOper \t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_pos.values[0, 7]}, \t\t{df_pos.values[0, 8]}, \t\t\t{df_pos.values[0, 9]}\
                        \n\
                        \nPos-платежи (гэмблинг и беттинг):  \nOper \t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_pos.values[0, 10]}, \t\t{df_pos.values[0, 11]}, \t\t\t{df_pos.values[0, 12]}\
                        \n\
                        \nPos-платежи (игорный бизнес):  \nOper \t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_pos.values[0, 13]}, \t\t\t\t\t\t\t\t\t\t\t{df_pos.values[0, 14]}, \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t{df_pos.values[0, 15]}\
                        \n\
                        \nЕРИП-платежи:  \nOper \t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_erip.values[0, 1]}, \t\t{df_erip.values[0, 2]}, \t\t\t{df_erip.values[0, 3]}\
                        \n\
                        \nVcard-платежи:  \nOper \t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_vcard.values[0, 0]}, \t\t\t\t{df_vcard.values[0, 1]}, \t\t\t\t\t\t{df_vcard.values[0, 2]}"})


def telegram_stats_bib(tbib_index):
    connection = psycopg2.connect(user=con_config['user_postgres'],
                                  password=con_config['password_postgres'],
                                  host=con_config['host_postgres'],
                                  port=con_config['port_postgres'],
                                  database=con_config['database_postgres'])

    print("Database opened successfully")

    cur = connection.cursor()

    string_wallet = telegramStat_configs['wallet_data']
    string_bal = telegramStat_configs['bal_data']
    string_telega = telegramStat_configs['telega_data']
    string_cashbox = telegramStat_configs['cashbox_data']
    string_indicators = telegramStat_configs['indicators_data']
    string_azs = telegramStat_configs['azs_data']
    string_pos = telegramStat_configs['pos_data']
    string_erip = telegramStat_configs['erip_data']
    string_vcard = telegramStat_configs['vcard_data']
    string_new_wallet = telegramStat_configs['new_wallet_data']

    df_wal = pd.read_sql(string_wallet, connection)
    df_bal = pd.read_sql(string_bal, connection)
    df_teleg = pd.read_sql(string_telega, connection)
    df_cash = pd.read_sql(string_cashbox, connection)
    df_ind = pd.read_sql(string_indicators, connection)
    df_azs = pd.read_sql(string_azs, connection)
    df_pos = pd.read_sql(string_pos, connection)
    df_erip = pd.read_sql(string_erip, connection)
    df_vcard = pd.read_sql(string_vcard, connection)
    df_n_wal = pd.read_sql(string_new_wallet, connection)

    token_tel = "11111111111111111111111111111111111111111111111111111"
    url_t = "https://api.telegram.org/bot"

    token = token_tel
    url = url_t
    channel_id = telegramStats_keys_bib[tbib_index]
    url += token
    method = url + "/sendMessage"

    requests.post(method, data={
        "chat_id": channel_id,
        "text": f"\n########_Кошельки_########\
                        \n\
                        \nTotal(осн.)\t\t\t\tNew(осн.)\t\t\t\tBal(все)\t\t\t\n{df_wal.values[6, 2]},\t\t\t\t\t\t\t\t\t+{df_wal.values[6, 4]},\t\t\t\t\t\t\t\t\t\t\t\t\t{np.round((df_wal.values[6, 5] + df_wal.values[5, 5] + df_wal.values[4, 5] + df_wal.values[3, 5]), decimals=2)} \
                        \n\
                        \n{(df_wal.values[6, 1])}: \nTotal\t\t\t\t\t\t\t\tNew\t\t\t\t\t\t\tBal\t\t\t\t\t\t\n{df_wal.values[6, 2]},   +{df_wal.values[6, 4]},   \t{df_wal.values[6, 5]} \
                        \n\
                        \n{df_wal.values[5, 1]}: \nTotal\t\t\t\t\t\tNew\t\t\t\t\tBal\t\t\t\t\t\t\n{df_wal.values[5, 2]},\t\t\t+{df_wal.values[5, 4]},\t\t\t\t\t{df_wal.values[5, 5]}\
                        \n\
                        \n{df_wal.values[3, 1]}: \nTotal\t\tNew\t\t\t\tBal\n{df_wal.values[3, 2]},\t\t\t+{df_wal.values[3, 4]},\t\t\t\t\t{df_wal.values[3, 5]}\
                        \n\
                        \n{df_wal.values[8, 1]}: \nTotal\t\t\t\t\t\tNew\t\t\t\t\t\tBal\n{df_wal.values[8, 2]},\t\t\t\t\t\t+{df_wal.values[8, 4]},\t\t\t\t\t{df_wal.values[8, 5]}\
                        \n\
                        \n{df_wal.values[9, 1]}: \nTotal\t\t\t\t\t\tNew\t\t\t\t\t\tBal\n{df_wal.values[9, 2]},\t\t\t\t\t+{df_wal.values[9, 4]},\t\t\t\t\t\t{df_wal.values[9, 5]}\
                        \n\
                        \n{df_wal.values[10, 1]}: \nTotal\t\t\t\t\tNew\t\t\tBal\n{df_wal.values[10, 2]},\t\t\t\t\t\t\t\t\t\t\t+{df_wal.values[10, 4]},\t\t\t\t\t\t{df_wal.values[10, 5]}\
                        \n\
                        \nВсего кошельков: \nNew_wallet \t\t\t\t\tTotal_wallet  \n{df_n_wal.values[0, 0]},\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t{df_n_wal.values[0, 1]} \
                        \n\
                        \n#######_Остатки_#########\
                        \n\
                        \nОстатки на кошельках физ.лиц: \nSum \n{df_bal.values[0, 0]}\
                        \n\
                        \nОстатки на кошельках юр.лиц: \nSum \n{df_teleg.values[0, 0]}\
                        \n\
                        \n#######_Магазины_#######\
                        \n\
                        \nВсего по магазинам: \nOper  \t\t\t\tSum  \t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_ind.values[0, 1]},  \t\t {df_ind.values[0, 2]},   \t\t{df_teleg.values[0, 1]}\
                        \n\
                        \n{df_cash.values[0, 1]}: \nOper  \t\tSum  \t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_cash.values[0, 3]},  \t\t\t{df_cash.values[0, 4]}, \t\t\t\t{df_teleg.values[0, 2]}\
                        \n\
                        \n{df_cash.values[1, 1]}: \nOper  \t\tSum  \t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_cash.values[1, 3]},  \t{df_cash.values[1, 4]}, \t\t\t{df_teleg.values[0, 3]}\
                        \n\
                        \n{df_cash.values[2, 1]}: \nOper  \t\tSum  \t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_cash.values[2, 3]},  \t\t\t{df_cash.values[2, 4]}, \t\t\t{df_teleg.values[0, 4]}\
                        \n\
                        \n{df_cash.values[3, 1]}: \nOper  \t\tSum  \t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_cash.values[3, 3]},  \t\t\t{df_cash.values[3, 4]}, \t\t\t\t\t\t{df_teleg.values[0, 6]}\
                        \n\
                        \n{df_cash.values[4, 1]}: \nOper  \t\tSum  \t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_cash.values[4, 3]},  \t\t\t{df_cash.values[4, 4]}, \t\t\t\t\t\t{df_teleg.values[0, 5]}\
                        \n\
                        \n#######_АЗС(заправки)_#######\
                        \n\
                        \nАЗС(Всего):  \nOper \t\t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_azs.values[0, 0]}, \t\t\t\t{df_azs.values[0, 1]},\t\t\t\t\t\t{df_azs.values[0, 2]}\
                        \n\
                        \nАЗС(Белоруснефть):  \nOper \t\t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_azs.values[0, 6]}, \t\t\t\t\t\t{df_azs.values[0, 7]},\t\t\t\t\t\t{df_azs.values[0, 8]}\
                        \n\
                        \nАЗС(А-100):  \nOper \t\t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_azs.values[0, 3]}, \t\t\t\t\t\t{df_azs.values[0, 4]},\t\t\t\t\t\t\t\t{df_azs.values[0, 5]}\
                        \n\
                        \nАЗС(United Company):  \nOper \t\t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_Sum   \n{df_azs.values[0, 9]}, \t\t\t\t\t\t\t\t{df_azs.values[0, 10]},\t\t\t\t\t\t\t\t{df_azs.values[0, 11]}\
                        \n\
                        \n#######_Платежи_#######\
                        \n\
                        \nPos-платежи:  \nOper \t\t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_pos.values[0, 1]}, \t\t{df_pos.values[0, 2]}, \t\t\t{df_pos.values[0, 3]}\
                        \n\
                        \nPos-платежи (продажи товаров и услуг):  \nOper \t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_pos.values[0, 4]}, \t\t{df_pos.values[0, 5]}, \t\t\t{df_pos.values[0, 6]}\
                        \n\
                        \nPos-платежи (перевозка пассажиров):  \nOper \t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_pos.values[0, 7]}, \t\t{df_pos.values[0, 8]}, \t\t\t{df_pos.values[0, 9]}\
                        \n\
                        \nPos-платежи (гэмблинг и беттинг):  \nOper \t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_pos.values[0, 10]}, \t\t{df_pos.values[0, 11]}, \t\t\t{df_pos.values[0, 12]}\
                        \n\
                        \nPos-платежи (игорный бизнес):  \nOper \t\t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_pos.values[0, 13]}, \t\t\t\t\t\t\t\t\t\t\t{df_pos.values[0, 14]}, \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t{df_pos.values[0, 15]}\
                        \n\
                        \nЕРИП-платежи:  \nOper \t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_erip.values[0, 1]}, \t\t{df_erip.values[0, 2]}, \t\t\t{df_erip.values[0, 3]}\
                        \n\
                        \nVcard-платежи:  \nOper \t\t\tSum \t\t\t\t\t\t\t\t\t\t\t\t\t\t\tTotal_sum   \n{df_vcard.values[0, 0]}, \t\t\t\t{df_vcard.values[0, 1]}, \t\t\t\t\t\t{df_vcard.values[0, 2]}"})


class TelegramCheckSendler:
    def __init__(self, chat_id, message):
        self.chat_id = chat_id
        self.message = message

    def send_message(self):
        for ch_id in chat_id:
            try:
                token = token_tel
                url = url_t
                channel_id = ch_id
                url += token
                method = url + "/sendMessage"

                r = requests.post(method, data={
                    "chat_id": channel_id,
                    "text": self.message
                })

            except requests.RequestException as tfr:
                print(tfr)


stg_upload = TelegramCheckSendler(chat_id,
                                  f"Загрузка таблиц из сторонних БД '{current_date}' отработала успешно !")

diff_data_upload = TelegramCheckSendler(chat_id,
                                        f"Загрузка данных о кассах '{current_date}' отработала успешно !")

proc_data_upload = TelegramCheckSendler(chat_id,
                                        f"Процедуры для bi таблиц postgres '{current_date}' отработали успешно !")

data_transfer = TelegramCheckSendler(chat_id,
                                     f"Загрузка данных из Postgres в Clickhouse '{current_date}' отработала успешно !")

good_message = TelegramCheckSendler(chat_id,
                                    f"Загрузка данных '{current_date}' отработала успешно !")
