import pandas as pd
import psycopg2
import logging
from psycopg2.extras import execute_values


logging.basicConfig(
    level=logging.INFO,
    filename='db_connect_log.log',
    filemode='a',
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding='utf8')


def connect_to_db(host, database, user, password):
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        logging.info(f"Соединение с базой {database} установлено")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
        raise


def load_csv_from_csv_file_path(file_path):
    return pd.read_csv(file_path)


def replace_nan_with_none(df):    
    return df.map(lambda x: None if pd.isna(x) else x)


def clear_table_by_from_date(table_name, conn, from_date):
    try:
        cursor = conn.cursor()        
        sql = f"DELETE FROM dm.dm_f101_round_f_v2 WHERE from_date = '{from_date}'"
        cursor.execute(sql)
        logging.info(f"Данные в таблице {table_name} отчищены за {from_date}")
    except psycopg2.Error as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
        raise


def write_from_csv_to_db(df, table_name, conn):
    cursor = conn.cursor()    
    df = replace_nan_with_none(df)        
    data = [tuple(row) for _, row in df.iterrows()]    
    cols = ",".join([str(i) for i in df.columns.tolist()])    
    sql = f"INSERT INTO {table_name} ({cols}) VALUES %s"

    try:
        execute_values(cursor, sql, data)
        conn.commit()
        logging.info(f"Данные успешно загружены в таблицу {table_name}")
    except Exception as e:
        logging.error(f"Ошибка при вставке данных: {e}")
        conn.rollback()
    finally:
        cursor.close()


def close_conn_of_db(conn):
    conn.close()
    logging.info("Соединение с базой данных закрыто")


def main():
    csv_file_path = 'dm_f101_round_f.csv'
    db_table_name = 'dm.dm_f101_round_f_v2'
    db_conn_param = connect_to_db('localhost', 'DE', 'postgres', '1') 
    csv_file = load_csv_from_csv_file_path(csv_file_path)
    clear_table_by_from_date(db_table_name, db_conn_param, '2018-01-01')   
    write_from_csv_to_db(csv_file, db_table_name, db_conn_param)  
    close_conn_of_db(db_conn_param)

if __name__ == '__main__':
    main()
