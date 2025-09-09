import psycopg2

from class_task_groups.configs import con_config
from airflow.exceptions import AirflowException

# ##############################----Function for call procedure---###########################################


class ProceduresExecutor:
    def __init__(self, keys):
        self.keys = keys

    def call_procedure(self, index):
        params = [index]

        connection = psycopg2.connect(user=con_config['user_postgres'],
                                      password=con_config['password_postgres'],
                                      host=con_config['host_postgres'],
                                      port=con_config['port_postgres'],
                                      database=con_config['database_postgres'])

        for param in params:
            try:
                cursor = connection.cursor()
                cursor.execute(f'call {self.keys[param]}();')
                print(f'Процедура {param} запущена:\n')

            except Exception as fr:
                error_messages = ['out of memory', 'no space left on device', 'not enough space']
                error_message = str(fr).lower()

                if any(msg in error_message for msg in error_messages):
                    raise AirflowException("PostgreSQL Out of Memory or Space Error occurred.")
                else:
                    raise AirflowException(f"Database error: {fr}")
                quit()

            cursor.close()
        connection.commit()
        connection.close()
