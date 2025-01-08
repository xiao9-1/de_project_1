-- Создание схемы и таблиц


-- схема для слоя
create schema if not exists dm;


drop table if exists dm.procedure_logs;

-- создание таблицы для логов
create table if not exists dm.procedure_logs (
    log_id serial primary key,	-- Уникальный идентификатор лога
    log_name varchar(50) not null,	-- Имя лога
    log_time timestamp not null,	-- Время логирования    
    log_message varchar(255)	-- Сообщение(необязательно) 
);	


insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('Schema_DM, dm.procedure_logs', current_timestamp, 'Созданы схема DM и таблица для логов');


-- создание таблицы для первой витрины 
create table if not exists dm.dm_account_turnover_f(
	on_date date,
	account_rk numeric,
	credit_amount numeric(23, 8),
	credit_amount_rub numeric(23, 8),
	debet_amount numeric(23, 8),
	debet_amount_rub numeric(23, 8)
);

insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('dm_account_turnover_f', current_timestamp, 'Создана таблица для 1-ой витрины dm_account_turnover_f');

-- создание таблицы для второй витрины 
create table if not exists dm.dm_account_balance_f(
	on_date date,
	account_rk numeric,
	balance_out double precision,
	balance_out_rub double precision
);

-- лог
insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('dm_account_balance_f', current_timestamp, 'Создана таблица для 2-ой витрины dm_account_balance_f');
