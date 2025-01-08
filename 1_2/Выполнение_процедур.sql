-- отчистим обе витрины для демонстрации

truncate dm.dm_account_turnover_f;
truncate dm.dm_account_balance_f;

-- выполним заполнение первой витрины
do $$ 
declare
    curr_date DATE;
begin
    -- Цикл по всем датам в заданном периоде
    for curr_date in 
        select generate_series('01-01-2018'::date, '31-01-2018'::date, '1 day'::interval)::date
    loop
        -- Вызов процедуры для каждой даты
        call ds.fill_account_turnover_f(curr_date);
    end loop;
end $$;


-- проверим 
select * from dm.dm_account_turnover_f order by 1,2 ;

-- заполнение 2 витрины данными за 31 декабря 2017 из ft_balance_f
insert into dm.dm_account_balance_f(on_date, account_rk, balance_out, balance_out_rub)
select
	f.on_date,
	f.account_rk,
	f.balance_out,
	balance_out * coalesce(e.reduced_cource, 1) 
from
	ds.ft_balance_f f
left join
	ds.md_exchange_rate_d e
	on f.currency_rk = e.currency_rk
	and f.on_date <= e.data_actual_end_date
	and f.on_date >= e.data_actual_date
where
	f.on_date = '2017-12-31';


-- проверим, что данные добавились. В таблице ft_balance_f 114 строк, аналогичное кол-во добавилось и в витрину


select * from dm.dm_account_balance_f;


select * from ds.ft_balance_f;


insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('Проверка вставки', current_timestamp, 'В dm.dm_account_balance_f добавлено' || (select count(*) from dm.dm_account_balance_f)
													|| ' строк из ' || (select count(*) from ds.ft_balance_f) || ' из ds.ft_balance_f');


-- выполним вторую процедуру для каждого дня января
do $$ 
declare
    d DATE;
begin
    -- Цикл по всем датам в заданном периоде
    for d in 
        select generate_series('01-01-2018'::date, '31-01-2018'::date, '1 day'::interval)::date
    loop
        -- Вызов процедуры для каждой даты
        call ds.fill_account_balance_f(d);
    end loop;
end $$;


select * from dm.dm_account_balance_f   order by 2 , 1 asc ;


select * from dm.procedure_logs;