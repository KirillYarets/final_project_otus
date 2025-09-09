import os
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.decorators import task_group, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from pyspark.sql import SparkSession

from class_task_groups.archive_maker import ArchiveMaker
from class_task_groups.clickhouse_operations import clickhouse_upload, clickhouse_query_execution_1, clickhouse_query_execution_2, clickhouse_query_execution_3
from class_task_groups.configs import db_transfer_config
from class_task_groups.configs import ora_config, postgresOut_config, bitrix_config
from class_task_groups.configs import procedures_config, check_config, clickhouse_common_1, clickhouse_common_2, clickhouse_common_3
from class_task_groups.data_loader import DataLoader
from class_task_groups.different_data import check_data, pos_coordinates
from class_task_groups.nbrb_api import nbrb_api
from class_task_groups.procedures_executor import ProceduresExecutor
from class_task_groups.telegram_message_sendler import (stg_upload,
                                                        diff_data_upload,
                                                        proc_data_upload,
                                                        data_transfer,
                                                        good_message,
                                                        telegram_stats,
                                                        telegram_stats_bib,
                                                        telegram_error_log_message,
                                                        telegramStats_range_bib_index,
                                                        telegramStats_keys_bib,
                                                        telegramStats_range_keys_bib,
                                                        telegramStats_range_keys,
                                                        telegramStats_range_index,
                                                        telegramStats_keys,
                                                        )

# ##############################----Parameters---###########################################

dashboard_common_1_keys = list(clickhouse_common_1.keys())
common_dashboard_range_keys_1 = range(len(dashboard_common_1_keys))
common_dashboard_range_index_1 = []

dashboard_common_2_keys = list(clickhouse_common_2.keys())
common_dashboard_range_keys_2 = range(len(dashboard_common_2_keys))
common_dashboard_range_index_2 = []

dashboard_common_3_keys = list(clickhouse_common_3.keys())
common_dashboard_range_keys_3 = range(len(dashboard_common_3_keys))
common_dashboard_range_index_3 = []

report_date = (datetime.now() - timedelta(days=1))
report_month = (datetime.now() - timedelta(days=1)).strftime("%B")

df_year = report_date.year
df_month = report_month
df_day = report_date.day

os.environ['JAVA_HOME'] = '/opt/java/jdk-17.0.2'

spark = SparkSession.builder \
    .config('spark.driver.extraClassPath') \
    .config('spark.network.timeout', '600s') \
    .master('local') \
    .appName("Final") \
    .getOrCreate()

proc_del_keys = procedures_config['del_keys']
del_keys = range(len(proc_del_keys))
delRangeIndex = []

proc_upload_keys = procedures_config['upload_keys']
proc_keys = range(len(proc_upload_keys))
proc_range_index = []

proc_agr_keys = procedures_config['agregate_keys']
agr_keys = range(len(proc_agr_keys))
agrRangeIndex = []

tab_keys = list(ora_config.keys())
range_keys = range(len(tab_keys))
range_index = []

bitrix_keys = list(bitrix_config.keys())
bitrix_range_keys = range(len(bitrix_keys))
bitrix_range_index = []

postgres_out_keys = list(postgresOut_config.keys())
postgres_out_range_keys = range(len(postgres_out_keys))
postgres_out_range_index = []

check_keys = list(check_config.keys())
check_range_keys = range(len(check_keys))
check_range_index = []

arch_tab_keys = list(ora_config.keys())
arch_range_keys = range(len(arch_tab_keys))
arch_range_index = []

arch_bitrix_keys = list(bitrix_config.keys())
arch_bitrix_range_keys = range(len(arch_bitrix_keys))
arch_bitrix_range_index = []

arch_postgres_out_keys = list(postgresOut_config.keys())
arch_postgres_out_range_keys = range(len(arch_postgres_out_keys))
arch_postgres_out_range_index = []

# ##### for tranfer to clickhouse ###########
db_transfer_config_keys = list(db_transfer_config.keys())
db_transfer_range_keys = range(len(db_transfer_config_keys))
db_transfer_range_index = []

max_range_transfer = max(db_transfer_range_keys) + 1

half_range_transfer = max_range_transfer // 2

if max(db_transfer_range_keys) % 2 != 0:
    half_range_transfer += 1
print(half_range_transfer)

# print(max(db_transfer_range_keys) - half_range_transfer)


# ##### end  ###########

print('Ключи из конвигурационного файла: ', tab_keys)

# ##############################----Dag/Tasks---###########################################
args_ora = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'provide_context': True,
    'retry_delay': timedelta(minutes=5)
}

groups = []
groups_2 = []
groups_3 = []

arch_bitrix = ArchiveMaker(bitrix_config, arch_bitrix_keys, 'Bitrix')
arch_ora = ArchiveMaker(ora_config, arch_tab_keys, 'Oracle')
arch_postgres = ArchiveMaker(postgresOut_config, arch_postgres_out_keys, 'Postgres')

del_data = ProceduresExecutor(proc_del_keys)
upload_data = ProceduresExecutor(proc_upload_keys)
agr_data = ProceduresExecutor(proc_agr_keys)

upload_bitrix = DataLoader(bitrix_config, bitrix_keys, 'bitrix')
upload_oracle = DataLoader(ora_config, tab_keys, 'oracle')
upload_postgres = DataLoader(postgresOut_config, postgres_out_keys, 'postgres_out')

with DAG(
        '1_COPY_total_data_new_final',
        start_date=datetime(year=2024, month=10, day=9),
        # end_date=  datetime(2999, 10, 11,15, 56),
        schedule_interval='30 21 * * *',
        # schedule_interval='@once',
        catchup=True,
        default_args=args_ora,
        tags=['production_upload']
) as ora_dag:
    # ############################----Telegram tasks---###########################################

    tel_task = PythonOperator(
        task_id='good_message',
        python_callable=good_message.send_message
    )

    tel_stg_upload = PythonOperator(
        task_id='stg_upload_message',
        python_callable=stg_upload.send_message
    )

    tel_diff_data_upload = PythonOperator(
        task_id='diff_data_upload_message',
        python_callable=diff_data_upload.send_message
    )

    tel_proc_data_upload = PythonOperator(
        task_id='proc_data_upload_message',
        python_callable=proc_data_upload.send_message
    )

    tel_data_transfer = PythonOperator(
        task_id='data_transfer_message',
        python_callable=data_transfer.send_message
    )

    # @task_group(group_id='telegram_error_log_message')
    # def tg5():
    #     tel_error_log_message = PythonOperator(
    #         task_id='telegram_error_log_message',
    #         python_callable=telegram_error_log_message
    #     )
    # 
    #     tel_error_log_message_task = DummyOperator(task_id='telegram_stream', dag=ora_dag, trigger_rule='all_done')
    # 
    #     [tel_error_log_message >> tel_error_log_message_task]

    with TaskGroup(group_id='telegram_error_log_message') as tg5:
        tel_error_log_message_task = PythonOperator(
            task_id='log_error_message',
            python_callable=telegram_error_log_message
        )

        tel_error_log_message_task_dummy = DummyOperator(
            task_id='telegram_stream',
            trigger_rule='all_done'
        )

        tel_error_log_message_task >> tel_error_log_message_task_dummy

    groups_3.append(tg5)

    # ############################----API data tasks---###########################################

    # @task_group(group_id='api_cur_nbrb')
    # def tg1():
    #
    #     api_nbrb = PythonOperator(
    #         task_id='api_nbrb',
    #         python_callable=nbrb_api
    #     )
    #
    #     t_api_nbrb = DummyOperator(task_id='strim_api', dag=ora_dag, trigger_rule='all_done')
    #
    #     [api_nbrb >> t_api_nbrb]
    #
    #
    # groups.append(tg1())

    with TaskGroup(group_id='api_cur_nbrb') as tg1:
        api_nbrb = PythonOperator(task_id='api_nbrb', python_callable=nbrb_api)

        t_api_nbrb = DummyOperator(task_id='strim_api', trigger_rule='all_done')

        api_nbrb >> t_api_nbrb

    groups.append(tg1)

    # ############################----Oracle tasks---###########################################

    with TaskGroup(group_id='upload_Oracle_data') as tg2:
        for index in range_keys:
            range_index.append(PythonOperator(
                task_id=tab_keys[index],
                dag=ora_dag,
                provide_context=True,
                python_callable=upload_oracle.upload_data,
                op_args=[index]))
            if index not in [0]:
                range_index[index - 1] >> range_index[index]

    # ##############################----Bitrix tasks---###########################################

    with TaskGroup(group_id='upload_Bitrix_data') as tg3:
        for b_index in bitrix_range_keys:
            bitrix_range_index.append(PythonOperator(
                task_id=bitrix_keys[b_index],
                dag=ora_dag,
                provide_context=True,
                python_callable=upload_bitrix.upload_data,
                op_args=[b_index]))
            if b_index not in [0]:
                bitrix_range_index[b_index - 1] >> bitrix_range_index[b_index]


    # @task_group(group_id='bitrix_fail_check')
    # def bitrix_fail_check():
    # 
    #     bitrix_check = DummyOperator(task_id='bitrix_check', dag=ora_dag, trigger_rule=TriggerRule.ALL_DONE)
    # 
    #     [bitrix_check]
    # 
    # 
    # groups_2.append(bitrix_fail_check())

    with TaskGroup(group_id='bitrix_fail_check') as bitrix_fail_check:
        bitrix_check = DummyOperator(task_id='bitrix_check', trigger_rule=TriggerRule.ALL_DONE)

    groups_2.append(bitrix_fail_check)

    # ##############################----Postgres tasks---###########################################

    with TaskGroup(group_id='upload_Postgres_data') as tg4:
        for p_index in postgres_out_range_keys:
            postgres_out_range_index.append(PythonOperator(
                task_id=postgres_out_keys[p_index],
                dag=ora_dag,
                provide_context=True,
                python_callable=upload_postgres.upload_data,
                op_args=[p_index]))
            if p_index not in [0]:
                postgres_out_range_index[p_index - 1] >> postgres_out_range_index[p_index]

    # ############################----Delete tasks---###########################################

    with TaskGroup(group_id='delete_temporary_data') as tdel:
        for index in del_keys:
            delRangeIndex.append(PythonOperator(
                task_id=proc_del_keys[index],
                dag=ora_dag,
                provide_context=True,
                python_callable=del_data.call_procedure,
                op_args=[index]))
            if index not in [0]:
                delRangeIndex[index - 1] >> delRangeIndex[index]

    with TaskGroup(group_id='stat_message') as tgTelegramStat:
        for t_index in telegramStats_range_keys:
            telegramStats_range_index.append(PythonOperator(
                task_id=telegramStats_keys[t_index],
                dag=ora_dag,
                provide_context=True,
                python_callable=telegram_stats,
                op_args=[t_index]))
            if t_index not in [0]:
                telegramStats_range_index[t_index - 1] >> telegramStats_range_index[t_index]

    with TaskGroup(group_id='bib_stat_message') as bib_tgTelegramStat:
        for tbib_index in telegramStats_range_keys_bib:
            telegramStats_range_bib_index.append(PythonOperator(
                task_id=telegramStats_keys_bib[tbib_index],
                dag=ora_dag,
                provide_context=True,
                python_callable=telegram_stats_bib,
                op_args=[tbib_index]))
            if tbib_index not in [0]:
                telegramStats_range_bib_index[tbib_index - 1] >> telegramStats_range_bib_index[tbib_index]

    # ############################----Processing data tasks---###########################################

    with TaskGroup(group_id='processing_data') as tgProcData:
        for procIndex in proc_keys:
            proc_range_index.append(PythonOperator(
                task_id=proc_upload_keys[procIndex],
                dag=ora_dag,
                provide_context=True,
                python_callable=upload_data.call_procedure,
                op_args=[procIndex]))
            if procIndex not in [0]:
                proc_range_index[procIndex - 1] >> proc_range_index[procIndex]

    # ############################----Agregation data tasks---###########################################

    with TaskGroup(group_id='agregation_data') as tgAgrData:
        for agrIndex in agr_keys:
            agrRangeIndex.append(PythonOperator(
                task_id=proc_agr_keys[agrIndex],
                dag=ora_dag,
                provide_context=True,
                python_callable=agr_data.call_procedure,
                op_args=[agrIndex]))
            if agrIndex not in [0]:
                agrRangeIndex[agrIndex - 1] >> agrRangeIndex[agrIndex]

    with TaskGroup(group_id='postgres_clickhouse_data_transfer_1') as tgDbTransfer:
        # db_transfer_range_index = range(0,56)
        for click_index in range(0, half_range_transfer):
            db_transfer_range_index.append(PythonOperator(
                task_id=db_transfer_config_keys[click_index],
                dag=ora_dag,
                provide_context=True,
                python_callable=clickhouse_upload,
                op_args=[click_index]))
            if click_index not in [0]:
                db_transfer_range_index[click_index - 1] >> db_transfer_range_index[click_index]

    with TaskGroup(group_id='postgres_clickhouse_data_transfer_2') as tgDbTransfer_2:
        db_transfer_range_index_2 = []
        for click_index in range(half_range_transfer, max_range_transfer):
            db_transfer_range_index_2.append(PythonOperator(
                task_id=db_transfer_config_keys[click_index],
                dag=ora_dag,
                provide_context=True,
                python_callable=clickhouse_upload,
                op_args=[click_index]))
            if click_index not in [half_range_transfer]:
                db_transfer_range_index_2[click_index - (half_range_transfer + 1)] >> db_transfer_range_index_2[
                    click_index - half_range_transfer]

    # ############################----Dashboards task---######################################

    with TaskGroup(group_id='dashboards_query_execution_1') as tgDashboardsExecution_1:
        for dashboard_query_index_1 in common_dashboard_range_keys_1:
            common_dashboard_range_index_1.append(PythonOperator(
                task_id=dashboard_common_1_keys[dashboard_query_index_1],
                dag=ora_dag,
                provide_context=True,
                python_callable=clickhouse_query_execution_1,
                op_args=[dashboard_query_index_1]))
            if dashboard_query_index_1 not in [0]:
                common_dashboard_range_index_1[dashboard_query_index_1 - 1] >> common_dashboard_range_index_1[dashboard_query_index_1]

    with TaskGroup(group_id='dashboards_query_execution_2') as tgDashboardsExecution_2:
        for dashboard_query_index_2 in common_dashboard_range_keys_2:
            common_dashboard_range_index_2.append(PythonOperator(
                task_id=dashboard_common_2_keys[dashboard_query_index_2],
                dag=ora_dag,
                provide_context=True,
                python_callable=clickhouse_query_execution_2,
                op_args=[dashboard_query_index_2]))
            if dashboard_query_index_2 not in [0]:
                common_dashboard_range_index_2[dashboard_query_index_2 - 1] >> common_dashboard_range_index_2[dashboard_query_index_2]

    with TaskGroup(group_id='dashboards_query_execution_3') as tgDashboardsExecution_3:
        for dashboard_query_index_3 in common_dashboard_range_keys_3:
            common_dashboard_range_index_3.append(PythonOperator(
                task_id=dashboard_common_3_keys[dashboard_query_index_3],
                dag=ora_dag,
                provide_context=True,
                python_callable=clickhouse_query_execution_3,
                op_args=[dashboard_query_index_3]))
            if dashboard_query_index_3 not in [0]:
                common_dashboard_range_index_3[dashboard_query_index_3 - 1] >> common_dashboard_range_index_3[dashboard_query_index_3]



    # ############################----STG Check data tasks---###########################################

    with TaskGroup(group_id='different_data') as diff_data:
        api_cord = PythonOperator(
            task_id='api',
            python_callable=pos_coordinates
        )

        

        t_api_pos = DummyOperator(task_id='strim_api_pos', dag=ora_dag, trigger_rule='all_done')

        [api_cord >> t_api_pos]


        for index in check_range_keys:
            check_range_index.append(PythonOperator(
                task_id=f'check_{check_keys[index]}',
                dag=ora_dag,
                provide_context=True,
                python_callable=check_data,
                op_args=[index]))
            if index not in [0]:
                check_range_index[index - 1] >> check_range_index[index]

    # ###########################----Oracle tasks---###########################################

    with TaskGroup(group_id='arch_Oracle_data') as archOra:
        for ora_index in arch_range_keys:
            arch_range_index.append(PythonOperator(
                task_id=arch_tab_keys[ora_index],
                dag=ora_dag,
                provide_context=True,
                python_callable=arch_ora.archivate,
                op_args=[ora_index]))
            if ora_index not in [0]:
                arch_range_index[ora_index - 1] >> arch_range_index[ora_index]

    # ##############################----Bitrix tasks---###########################################

    with TaskGroup(group_id='arch_Bitrix_data') as archBitr:
        for bit_index in arch_bitrix_range_keys:
            arch_bitrix_range_index.append(PythonOperator(
                task_id=arch_bitrix_keys[bit_index],
                dag=ora_dag,
                provide_context=True,
                python_callable=arch_bitrix.archivate,
                op_args=[bit_index]))
            if bit_index not in [0]:
                arch_bitrix_range_index[bit_index - 1] >> arch_bitrix_range_index[bit_index]

    # ##############################----Postgres tasks---###########################################

    with TaskGroup(group_id='arch_Postgres_data') as archPost:
        for post_index in arch_postgres_out_range_keys:
            arch_postgres_out_range_index.append(PythonOperator(
                task_id=arch_postgres_out_keys[post_index],
                dag=ora_dag,
                provide_context=True,
                python_callable=arch_postgres.archivate,
                op_args=[post_index]))
            if post_index not in [0]:
                arch_postgres_out_range_index[post_index - 1] >> arch_postgres_out_range_index[post_index]

tStart = DummyOperator(task_id='start', dag=ora_dag)
tEnd = DummyOperator(task_id='end', dag=ora_dag)
# tPass = DummyOperator(task_id='pass', dag=ora_dag)
# tPass2 = DummyOperator(task_id='pass_2', dag=ora_dag)
# tTest = DummyOperator(task_id='test', dag=ora_dag)

# tApiNBRB = DummyOperator(task_id='strim_api', dag=ora_dag, trigger_rule = 'all_done')

tStart >> tdel >> tg2 >> tel_stg_upload >> diff_data >> tel_diff_data_upload >> tgProcData >> tel_proc_data_upload >> [
    tgAgrData] >> tel_task >> [
    tgTelegramStat, bib_tgTelegramStat] >> tEnd >> [groups_3[0]]

tStart >> tdel >> tg2 >> tel_stg_upload >> diff_data >> tel_diff_data_upload >> tgProcData >> tel_proc_data_upload >> [
    tgDbTransfer,
    tgDbTransfer_2] >> tel_data_transfer >> [tgDashboardsExecution_3,
                                             tgDashboardsExecution_2,
                                             tgDashboardsExecution_1] >> tel_task >> [
    tgTelegramStat, bib_tgTelegramStat] >> tEnd >> [groups_3[0]]

tStart >> tdel >> tg3 >> groups_2[
    0] >> tel_stg_upload >> diff_data >> tel_diff_data_upload >> tgProcData >> tel_proc_data_upload >> [
    tgAgrData] >> tel_task >> [
    tgTelegramStat, bib_tgTelegramStat] >> tEnd >> [groups_3[0]]

tStart >> tdel >> tg3 >> groups_2[
    0] >> tel_stg_upload >> diff_data >> tel_diff_data_upload >> tgProcData >> tel_proc_data_upload >> [tgDbTransfer,
                                                                                                        tgDbTransfer_2] >> tel_data_transfer >> [
    tgDashboardsExecution_3,
    tgDashboardsExecution_2,
    tgDashboardsExecution_1] >> tel_task >> [
    tgTelegramStat, bib_tgTelegramStat] >> tEnd >> [groups_3[0]]

tStart >> tdel >> tg4 >> tel_stg_upload >> diff_data >> tel_diff_data_upload >> tgProcData >> tel_proc_data_upload >> [
    tgAgrData] >> tel_task >> [
    tgTelegramStat, bib_tgTelegramStat] >> tEnd >> [groups_3[0]]

tStart >> tdel >> tg4 >> tel_stg_upload >> diff_data >> tel_diff_data_upload >> tgProcData >> tel_proc_data_upload >> [
    tgDbTransfer,
    tgDbTransfer_2] >> tel_data_transfer >> [tgDashboardsExecution_3,
                                             tgDashboardsExecution_2,
                                             tgDashboardsExecution_1] >> tel_task >> [
    tgTelegramStat, bib_tgTelegramStat] >> tEnd >> [groups_3[0]]

tStart >> [groups[0]] >> tEnd >> [archOra, archBitr, archPost]
