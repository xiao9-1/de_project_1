# airflow библиотеки
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# python библиотеки
import pandas as pd
import logging
import time
from datetime import datetime


default_args = {
    'owner': 'postgres',
    'start_date': datetime(2024, 12, 28),
    'retries': 2
}


path_to_csv = Variable.get("file_path", default_var="/home/kir/airflow/data/")


# Функция для заполнения таблицы с логами
def log_to_db(task_id, start_time, end_time, status, message,):
    try:
        postgres_hook = PostgresHook(postgres_conn_id="pg-db")
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        
        sql_script = """
        INSERT INTO airflow_logs.af_logs (task_id, start_time, end_time, status, log_message)
        VALUES (%s, %s, %s, %s, %s);
        """
        cursor.execute(sql_script, (task_id, start_time, end_time, status, message))
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Ошибка записи лога: {e}")


# Функция для логирования старта DAG
def log_start(task_id):
    start_time = datetime.now()
    log_to_db(task_id, start_time, start_time, 'START', f'Запуск задачи {task_id}')


# Функция для логирования завершения DAG
def log_end(task_id):
    end_time = datetime.now()
    log_to_db(task_id, end_time, end_time, 'END', f'Завершение задачи {task_id}')

# Функция для коннекта к постгресу
def get_postgres_connection():
    postgres_hook = PostgresHook(postgres_conn_id="pg-db")
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    return connection, cursor


def update_or_insert_ft_balance_f(): 

    start_time = start_time = datetime.now()
    connection, cursor = None, None

    try:
        # Чтение данных из CSV
        csv_path = f"{path_to_csv}ft_balance_f.csv"
        data = pd.read_csv(csv_path, delimiter=";")
        data.columns = data.columns.str.lower()

        connection, cursor = get_postgres_connection()

        # Обработка строк из CSV
        for _, row in data.iterrows():
            sql_script = """
            INSERT INTO ds.ft_balance_f (on_date, account_rk, currency_rk, balance_out)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (on_date, account_rk) DO UPDATE SET
                currency_rk = EXCLUDED.currency_rk,
                balance_out = EXCLUDED.balance_out;
            """                       
            # Передача значений в SQL-запрос
            cursor.execute(sql_script, (row['on_date'], row['account_rk'], row['currency_rk'], row['balance_out']))
            
        # Коммит изменений
        connection.commit()

        end_time = datetime.now()
        log_to_db('update_or_insert_ft_balance_f', start_time, end_time,  'SUCCESS', f'Данные успешно загружены.')
    except Exception as e:
        end_time = datetime.now()        
        log_to_db('update_or_insert_ft_balance_f', start_time, end_time,  'ERROR', f'Ошибка: {e}')
        raise
    finally:        
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def update_or_insert_ft_posting_f():

    start_time = datetime.now()

    try:    
        # Очищает таблицу перед вставкой новых данных.
        postgres_hook = PostgresHook(postgres_conn_id="pg-db")
        sql = f"TRUNCATE TABLE ds.ft_posting_f"
        postgres_hook.run(sql)

        ft_posting_f_table = pd.read_csv(f"{path_to_csv}ft_posting_f.csv", delimiter=";")
        ft_posting_f_table.columns = ft_posting_f_table.columns.str.lower() 

        # Получаем engine для SQLAlchemy
        engine = postgres_hook.get_sqlalchemy_engine()

        # Вставка данных в таблицу
        ft_posting_f_table.to_sql('ft_posting_f', engine, schema="ds", if_exists='append', index=False)

        end_time = datetime.now()        
        log_to_db('update_or_insert_ft_posting_f', start_time, end_time,  'SUCCESS', f'Данные успешно загружены.')
    except Exception as e:
        end_time = datetime.now()
        log_to_db('update_or_insert_ft_posting_f', start_time, end_time, 'ERROR', f'Ошибка при загрузке данных: {e}')        
        raise


def update_or_insert_md_account_d():

    start_time = datetime.now()
    connection, cursor = None, None

    try:
        # Чтение данных из CSV
        csv_path = f"{path_to_csv}md_account_d.csv"
        data = pd.read_csv(csv_path, delimiter=";")
        data.columns = data.columns.str.lower()

        connection, cursor = get_postgres_connection()
        
        # Обработка строк из CSV
        for _, row in data.iterrows():
            sql_script = """
            INSERT INTO ds.md_account_d (data_actual_date, data_actual_end_date, account_rk,
                                        account_number, char_type, currency_rk, currency_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (data_actual_date, account_rk) DO UPDATE SET
                data_actual_end_date = EXCLUDED.data_actual_end_date,
                account_number = EXCLUDED.account_number,
                char_type = EXCLUDED.char_type,
                currency_rk = EXCLUDED.currency_rk,
                currency_code = EXCLUDED.currency_code
            """
            # Передача значений в SQL-запрос
            cursor.execute(sql_script, (row['data_actual_date'], row['data_actual_end_date'], row['account_rk'],
                                        row['account_number'], row['char_type'], row['currency_rk'], row['currency_code']))

        # Коммит изменений
        connection.commit()
        end_time = datetime.now()        
        print("Данные успешно вставлены или обновлены.")
        log_to_db('update_or_insert_md_account_d', start_time, end_time, 'SUCCESS', 'Данные успешно загружены')
    except Exception as e:
        log_to_db('update_or_insert_md_account_d', start_time, end_time, 'ERROR', f'Ошибка при загрузке данных: {e}')
        raise
    finally:
        # Закрытие соединений, даже если произошла ошибка
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def update_or_insert_md_currency_d():
    start_time = datetime.now()
    connection, cursor = None, None    
    try:
        # Чтение данных из CSV
        csv_path = f"{path_to_csv}md_currency_d.csv"
        data = pd.read_csv(csv_path, delimiter=";",dtype= {'currency_code': str}, encoding='cp1252')
        print(data.columns)
        data.columns = data.columns.str.lower()   

        data['code_iso_char'] = data['code_iso_char'].apply(lambda x: '' if x == '˜' or x == '' or pd.isna(x) else x)               
        #data['currency_code'] = data['currency_code'].apply(lambda x: '' if x == '' or pd.isna(x) else x)

        # Преобразование столбца с проверкой на NaN
        data['currency_code'] = data['currency_code'].apply(
            lambda x: str(int(float(x))).zfill(3) if pd.notna(x) else '')
        
        data.to_csv(csv_path, index=False, sep=';', encoding='cp1252')
        connection, cursor = get_postgres_connection()             
        
        for _, row in data.iterrows():
            row = row.apply(lambda x: None if x == '' or pd.isna(x) else x)            
            sql_script = """
            INSERT INTO ds.md_currency_d (currency_rk, data_actual_date, data_actual_end_date,
                                            currency_code, code_iso_char)
            VALUES (%s, %s, %s, %s::varchar(3), %s)
            ON CONFLICT (currency_rk, data_actual_date) DO UPDATE SET
                data_actual_end_date = EXCLUDED.data_actual_end_date,
                currency_code = EXCLUDED.currency_code,
                code_iso_char = EXCLUDED.code_iso_char;                
            """
            # Передача значений в SQL-запрос
            cursor.execute(sql_script, (row.get('currency_rk'), row.get('data_actual_date'),
                                        row.get('data_actual_end_date'), row.get('currency_code'), 
                                        row.get('code_iso_char')))        
        
        connection.commit() 

        end_time = datetime.now()       
        log_to_db('update_or_insert_md_currency_d', start_time, end_time, 'SUCCESS', 'Данные успешно загружены')
    except Exception as e:
        log_to_db('update_or_insert_md_currency_d', start_time, end_time, 'ERROR', f'Ошибка при загрузке данных: {e}')
        raise
    finally:        
        if cursor:
            cursor.close()
        if connection:
            connection.close()



def update_or_insert_md_exchange_rate_d():

    start_time = datetime.now()
    connection, cursor = None, None

    try:
        # Чтение данных из CSV
        csv_path = f"{path_to_csv}md_exchange_rate_d.csv"
        data = pd.read_csv(csv_path, delimiter=";")
        data.columns = data.columns.str.lower()

        connection, cursor = get_postgres_connection()
        
        # Обработка строк из CSV
        for _, row in data.iterrows():
            sql_script = """
            INSERT INTO ds.md_exchange_rate_d (data_actual_date, data_actual_end_date, currency_rk,
                                            reduced_cource, code_iso_num)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (data_actual_date, currency_rk) DO UPDATE SET
                data_actual_end_date = EXCLUDED.data_actual_end_date,
                reduced_cource = EXCLUDED.reduced_cource,
                code_iso_num = EXCLUDED.code_iso_num;                
            """
            # Передача значений в SQL-запрос
            cursor.execute(sql_script, (row['data_actual_date'], row['data_actual_end_date'], row['currency_rk'],
                                        row['reduced_cource'], row['code_iso_num']))
        # Коммит изменений
        connection.commit() 
        end_time = datetime.now()       
        log_to_db('update_or_insert_md_exchange_rate_d', start_time, end_time, 'SUCCESS', 'Данные успешно загружены')
    except Exception as e:
        log_to_db('update_or_insert_md_exchange_rate_d', start_time, end_time, 'ERROR', f'Ошибка при загрузке данных: {e}')
        raise
    finally:        
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def update_or_insert_md_ledger_account_s():

    start_time = datetime.now()
    connection, cursor = None, None

    try:
        # Чтение данных из CSV
        csv_path = f"{path_to_csv}md_ledger_account_s.csv"
        data = pd.read_csv(csv_path, delimiter=";")
        data.columns = data.columns.str.lower()

        # Список всех необходимых столбцов(не работала функция при недостающих столбах, пришлось добавлять самостоятельно)
        required_columns = [
            'chapter', 'chapter_name', 'section_number', 'section_name', 'subsection_name', 
            'ledger1_account', 'ledger1_account_name', 'ledger_account', 'ledger_account_name', 
            'characteristic', 'is_resident', 'is_reserve', 'is_reserved', 'is_loan', 
            'is_reserved_assets', 'is_overdue', 'is_interest', 'pair_account', 'start_date', 
            'end_date', 'is_rub_only', 'min_term', 'min_term_measure', 'max_term', 
            'max_term_measure', 'ledger_acc_full_name_translit', 'is_revaluation', 'is_correct'
        ]

        # Добавляем недостающие столбцы в DataFrame начиная с 'end_date'
        for col in required_columns[required_columns.index('end_date'):]:
            if col not in data.columns:
                data[col] = pd.NA  # Добавляем пустой столбец

        # Сохраняем измененный DataFrame обратно в CSV
        data.to_csv(csv_path, sep=";", index=False)
        print(f"Новые столбцы добавлены в CSV: {', '.join(required_columns[required_columns.index('end_date'):])}")

        # Открываем соединение с базой данных
        connection, cursor = get_postgres_connection()

        # Обработка строк из CSV
        for _, row in data.iterrows():
            # Замена пандовских нанов на sql null
            row = row.apply(lambda x: None if pd.isna(x) else x)

            sql_script = """
            INSERT INTO ds.md_ledger_account_s (chapter, chapter_name, section_number, section_name, subsection_name,
                                                ledger1_account, ledger1_account_name, ledger_account, ledger_account_name,
                                                characteristic, is_resident, is_reserve, is_reserved, is_loan, 
                                                is_reserved_assets, is_overdue, is_interest, pair_account, start_date, 
                                                end_date, is_rub_only, min_term, min_term_measure, max_term, 
                                                max_term_measure, ledger_acc_full_name_translit, is_revaluation, is_correct)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ledger_account, start_date) DO UPDATE SET
                chapter = EXCLUDED.chapter,
                chapter_name = EXCLUDED.chapter_name,
                section_number = EXCLUDED.section_number,
                section_name = EXCLUDED.section_name,
                subsection_name = EXCLUDED.subsection_name,
                ledger1_account = EXCLUDED.ledger1_account,
                ledger1_account_name = EXCLUDED.ledger1_account_name,
                ledger_account_name = EXCLUDED.ledger_account_name,
                characteristic = EXCLUDED.characteristic,
                is_resident = EXCLUDED.is_resident,
                is_reserve = EXCLUDED.is_reserve,
                is_reserved = EXCLUDED.is_reserved,
                is_loan = EXCLUDED.is_loan,
                is_reserved_assets = EXCLUDED.is_reserved_assets,
                is_overdue = EXCLUDED.is_overdue,
                is_interest = EXCLUDED.is_interest,
                pair_account = EXCLUDED.pair_account,
                end_date = EXCLUDED.end_date,
                is_rub_only = EXCLUDED.is_rub_only,
                min_term = EXCLUDED.min_term,
                min_term_measure = EXCLUDED.min_term_measure,
                max_term = EXCLUDED.max_term,
                max_term_measure = EXCLUDED.max_term_measure,
                ledger_acc_full_name_translit = EXCLUDED.ledger_acc_full_name_translit,
                is_revaluation = EXCLUDED.is_revaluation,
                is_correct = EXCLUDED.is_correct;
            """
            # Передача значений в SQL-запрос
            cursor.execute(sql_script, (
                row.get('chapter'), row.get('chapter_name'), row.get('section_number'), row.get('section_name'),
                row.get('subsection_name'), row.get('ledger1_account'), row.get('ledger1_account_name'),
                row.get('ledger_account'), row.get('ledger_account_name'), row.get('characteristic'),
                row.get('is_resident'), row.get('is_reserve'), row.get('is_reserved'), row.get('is_loan'),
                row.get('is_reserved_assets'), row.get('is_overdue'), row.get('is_interest'), row.get('pair_account'),
                row.get('start_date'), row.get('end_date'), row.get('is_rub_only'), row.get('min_term'),
                row.get('min_term_measure'), row.get('max_term'), row.get('max_term_measure'),
                row.get('ledger_acc_full_name_translit'), row.get('is_revaluation'), row.get('is_correct')
            ))
        # Коммит изменений
        connection.commit()

        end_time = datetime.now()
        log_to_db('update_or_insert_md_ledger_account_s', start_time, end_time, 'SUCCESS', 'Данные успешно загружены')
    except Exception as e:
        log_to_db('update_or_insert_md_ledger_account_s', start_time, end_time, 'ERROR', f'Ошибка при загрузке данных: {e}')
        raise
    finally:        
        if cursor:
            cursor.close()
        if connection:
            connection.close()


with DAG(
    'insert_data_to_PG',
    default_args=default_args,
    description='Загрузка данных из csv в PG',
    catchup=False,
    schedule_interval=None
) as dag:
    
    # Операторы для начала и конца DAG
    start = PythonOperator(
        task_id='Start',
        python_callable=log_start,
        op_args=['Start']
    )

    end = PythonOperator(
        task_id='End',
        python_callable=log_end,
        op_args=['End']
    )

    
    sleep_operator = PythonOperator(
    task_id='sleep_task',
    python_callable=lambda : time.sleep(5),
    dag=dag    
    )

    ft_balance_f_operator = PythonOperator(
        task_id="ft_balance_f_insert_or_update",
        python_callable=update_or_insert_ft_balance_f,
        op_kwargs={"table_name": "ft_balance_f"}
    )

    ft_posting_f_operator = PythonOperator(
        task_id="ft_posting_f_insert_or_update",
        python_callable=update_or_insert_ft_posting_f,
        op_kwargs={"table_name": "ft_posting_f"}
    )

    md_account_d_operator = PythonOperator(
        task_id="md_account_d_insert_or_update",
        python_callable=update_or_insert_md_account_d,
        op_kwargs={"table_name": "md_account_d"}
    )
    
    md_currency_d_operator = PythonOperator(
        task_id="md_currency_d_insert_or_update",
        python_callable=update_or_insert_md_currency_d,
        op_kwargs={"table_name": "md_currency_d"}
    )

    md_exchange_rate_d_operator = PythonOperator(
        task_id="md_exchange_rate_d_insert_or_update",
        python_callable=update_or_insert_md_exchange_rate_d,
        op_kwargs={"table_name": "md_exchange_rate_d"}
    )

    md_ledger_account_s_operator = PythonOperator(
        task_id="md_ledger_account_s_insert_or_update",
        python_callable=update_or_insert_md_ledger_account_s,
        op_kwargs={"table_name": "md_ledger_account_s"}
    )

    
    start >> sleep_operator >> [ft_balance_f_operator, ft_posting_f_operator, md_account_d_operator,
              md_currency_d_operator, md_exchange_rate_d_operator, md_ledger_account_s_operator] >> end
    
    