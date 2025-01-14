import pandas as pd
import psycopg2
import logging


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
        logging.info(f"Соединение с базой {database} данных установлено")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
        raise


def create_sql_query(query):
    return query


def read_sql(query, conn):
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        logging.error(f"Ошибка выполнения SQL-запроса: {e}")
        raise


def transform_to_csv(df, path):
    try:
        df.to_csv(path, index=False)
        logging.info(f"Данные успешно сохранены в файл: {path}")
    except Exception as e:
        logging.error(f"Ошибка сохранения данных в CSV: {e}")
        raise


def close_conn(conn):
    try:
        conn.close()
        logging.info("Соединение с базой данных закрыто")
    except Exception as e:
        logging.error(f"Ошибка при закрытии соединения: {e}")


def main():    
    db_conn_param = connect_to_db('localhost', 'DE', 'postgres', '1')              
    input_query = create_sql_query('SELECT * FROM dm.dm_f101_round_f')
    read_input_query = read_sql(input_query, db_conn_param)
    transform_to_csv(read_input_query, 'dm_f101_round_f.csv')


if __name__ == '__main__':
    main()
