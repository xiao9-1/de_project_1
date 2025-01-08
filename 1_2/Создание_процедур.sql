-- СОЗДАНИЕ ПЕРВОЙ ПРОЦЕДУРЫ ds.fill_account_turnover_f

--лог
insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('fill_account_turnover_f', current_timestamp, 'Начало выполнения кода по созданию 1 процедуры');

-- процедура 1
create or replace procedure ds.fill_account_turnover_f(i_ondate date)
language plpgsql
as $$
begin
    -- clear by input date
    delete from dm.dm_account_turnover_f
    where on_date = i_ondate;

	-- Логирование начала процедуры
    insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('fill_account_turnover_f', current_timestamp, 'Начало заполнения проводки по аккаунтам для даты: ' || i_ondate);
	
    -- insert by input date
    insert into dm.dm_account_turnover_f(
        on_date, account_rk, credit_amount,
        credit_amount_rub, debet_amount, debet_amount_rub
    )
	select
    p.oper_date as on_date,
    b.account_rk as account_rk,
    sum(case when b.account_rk = p.credit_account_rk then p.credit_amount else 0 end) as credit_amount,
	
    coalesce(sum(case when b.account_rk = p.credit_account_rk then p.credit_amount else 0 end) * ex.reduced_cource, 
             sum(case when b.account_rk = p.credit_account_rk then p.credit_amount else 0 end) * 1) as credit_amount_rub,
			 
    sum(case when b.account_rk = p.debet_account_rk then p.debet_amount else 0 end) as debet_amount,
	
    coalesce(sum(case when b.account_rk = p.debet_account_rk then p.debet_amount else 0 end) * ex.reduced_cource, 
             sum(case when b.account_rk = p.debet_account_rk then p.debet_amount else 0 end) * 1) as debet_amount_rub
	from
	    ds.ft_balance_f b
	left join 
	    ds.ft_posting_f p
	    on (b.account_rk = p.credit_account_rk) 
	    or (b.account_rk = p.debet_account_rk)
	left join 
	    ds.md_exchange_rate_d as ex
	    on ex.currency_rk = b.currency_rk
	    and ex.data_actual_date <= p.oper_date
	    and ex.data_actual_end_date >= p.oper_date
	where
		p.oper_date = i_ondate
	group by 
	    p.oper_date, 
	    b.account_rk,
	    ex.reduced_cource
	having
		(sum(case when b.account_rk = p.credit_account_rk then p.credit_amount else 0 end) != 0)
		and (sum(case when b.account_rk = p.debet_account_rk then p.debet_amount else 0 end) != 0);
end;
$$;

-- лог 
insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('fill_account_turnover_f', current_timestamp, 'Выполнение кода по созданию 1 процедуры завершено');


-- СОЗДАНИЕ ВТОРОЙ ПРОЦЕДУРЫ ds.fill_account_balance_f

-- Лог
insert into dm.procedure_logs (log_name, log_time, log_message)
	values ('fill_account_balance_f', current_timestamp, 'Выполнение кода по созданию 2 процедуры');

-- процедура 2
create or replace procedure ds.fill_account_balance_f(i_ondate date)
language plpgsql
as $$
declare
    v_prev_date date := i_ondate - interval '1 day';
begin	
    -- Удаление старых данных за указанную дату
    delete from dm.dm_account_balance_f
    where on_date = i_ondate;

	-- Логирование начала процедуры
    insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('fill_account_balance_f', current_timestamp, 'Начало заполнения остатка аккаунтов для даты: ' || i_ondate);
	
    -- Вставка рассчитанных данных
    insert into dm.dm_account_balance_f (
        on_date, account_rk, balance_out, balance_out_rub
    )
    select
        i_ondate as on_date,
        a.account_rk,
        case
            -- Для активных счетов
            when a.char_type = 'А' then
                coalesce(b.balance_out, 0) + coalesce(t.debet_amount, 0) - coalesce(t.credit_amount, 0)
            -- Для пассивных счетов
            when a.char_type = 'П' then
                coalesce(b.balance_out, 0) - coalesce(t.debet_amount, 0) + coalesce(t.credit_amount, 0)
        end as balance_out,
        case
			-- Для активных счетов в рублях
            when a.char_type = 'А' then
                coalesce(b.balance_out_rub, 0) + coalesce(t.debet_amount_rub, 0) - coalesce(t.credit_amount_rub, 0)
			-- Для пассивных счетов в рублях 
            when a.char_type = 'П' then
                coalesce(b.balance_out_rub, 0) - coalesce(t.debet_amount_rub, 0) + coalesce(t.credit_amount_rub, 0)
        end as balance_out_rub
    from
        ds.md_account_d as a
    left join
        dm.dm_account_balance_f as b
    	on
        b.account_rk = a.account_rk
        and b.on_date = v_prev_date
    left join
        dm.dm_account_turnover_f as t
    	on
        t.account_rk = a.account_rk
        and t.on_date = i_ondate
    where
        i_ondate between a.data_actual_date and a.data_actual_end_date;	
end;
$$;

-- лог
insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('fill_account_balance_f', current_timestamp, 'Начало выполнения кода по созданию 2 процедуры завершено');