import concurrent.futures
import pandas as pd
from datetime import datetime

cast_functions = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "datetime": lambda x, fmt: pd.to_datetime(x, format=fmt)
}


# ----Function for data extraction---###########################################

def extract_expected_columns_and_date_formats(config, keys, index):
    expected_columns = config[f'{keys[index]}']['expected_columns']

    expected_types = []
    date_formats = {}

    for column in expected_columns:
        column_name = column["name"]
        column_type = column["type"]
        expected_types.append(column_type)

    if column_type == "datetime":
        date_formats[column_name] = column["date_format"]

    return expected_types, date_formats


# ----Function for row validation---###########################################

def validate_row(index, row, expected_types, date_formats):
    failed_row = None
    error_messages = []

    for column_name, expected_type in zip(row.index, expected_types):
        cell_value = row[column_name]

        if pd.isna(cell_value):
            continue

        try:
            if expected_type == "datetime":
                date_format = date_formats.get(column_name, None)
                if date_format is not None:
                    casted_value = cast_functions.get(expected_type, lambda x, fmt: x)(cell_value, date_format)
                else:
                    casted_value = cell_value
            else:
                casted_value = cast_functions.get(expected_type, lambda x: x)(cell_value)

        except (TypeError, ValueError):
            error_message = (f"Row {index}, Column '{column_name}':"
                             f" Value '{cell_value}' is not of the expected type {expected_type}")
            print(error_message)
            error_messages.append(error_message)

            failed_row = row
            break

    return failed_row, error_messages


# ----Function for dataframe validation---###########################################

def validate_dataframe(df, expected_types, date_formats):
    failed_rows = []
    error_messages = []

    # Use concurrent.futures.ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(validate_row, index, row, expected_types, date_formats) for index, row in
                   df.iterrows()]

        for future in concurrent.futures.as_completed(futures):
            failed_row, errors = future.result()
            if failed_row is not None:
                failed_rows.append(failed_row)
                error_messages.extend(errors)

    return failed_rows, error_messages


# ----Function to write errors to the database---###########################################

def write_errors_to_database(**kwargs):
    df = kwargs.get('df')
    failed_rows = kwargs.get('failed_rows')
    error_messages = kwargs.get('error_messages')
    process_name = kwargs.get('process_name')
    connection = kwargs.get('connection')
    config = kwargs.get('config')
    keys = kwargs.get('keys')
    index = kwargs.get('index')
    sql_statement_key = kwargs.get('sql_statement_key')

    if failed_rows:
        error_data_list = []
        error_log_message_list = []

        for _, (failed_row, error_message) in enumerate(zip(failed_rows, error_messages)):
            failed_row_df = pd.DataFrame([failed_row], columns=df.columns)

            error_data = {
                'error_date': datetime.now(),
                'proc_name': process_name,
                'table_name': 'oplati_stage_level.' + config[keys[index]]['postgres_table'],
                'json_data': failed_row_df.to_json(orient="records", force_ascii=False)
            }

            error_data_list.append(error_data)

            error_log_message_data = {
                'error_date': datetime.now(),
                'procedure_name': process_name,
                'error_code': None,
                'error_message': error_message,
                'error_context': 'SQL statement: ' + config[keys[index]][sql_statement_key]
            }

            error_log_message_list.append(error_log_message_data)

        error_data_df = pd.DataFrame(error_data_list)
        error_log_message_df = pd.DataFrame(error_log_message_list)

        error_data_df.to_sql(
            "error_data",
            con=connection,
            schema="oplati_statistic_error",
            if_exists="append",
            index=False
        )

        error_log_message_df.to_sql(
            "error_log_message",
            con=connection,
            schema="oplati_statistic_error",
            if_exists="append",
            index=False
        )
