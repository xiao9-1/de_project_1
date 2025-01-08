create schema airflow_logs;

CREATE TABLE airflow_logs.af_logs (
    log_id SERIAL PRIMARY KEY,
    task_id VARCHAR(255),		   -- Название ф-ии
    start_time TIMESTAMP,         -- Время начала
    end_time TIMESTAMP,           -- Время окончания
    status VARCHAR(50),           -- Статус выполнения задачи
    log_message TEXT,             -- Сообщение лога
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Время записи лога
);


select * from airflow_logs.af_logs
