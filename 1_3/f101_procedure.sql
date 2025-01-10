insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('fdm.dm_f101_round_f', current_timestamp, 'Создание процедуры для заполнения формы 101');


create or replace procedure dm.fill_f101_round_f(i_ondate date)
language plpgsql
as $$
declare v_to_date date; prev_date date;
begin

	delete from dm.dm_f101_round_f
	where from_date = i_ondate;
	
	v_to_date := i_ondate + interval '1 month' - interval '1 day';
	prev_date := i_ondate - interval '1 day';	
	
    -- вставляем данные в целевую таблицу
    insert into dm.dm_f101_round_f (
        from_date,
        to_date,
        chapter,
        ledger_account,
		characteristic,
		balance_in_rub,
		balance_in_val,
		balance_in_total,
		turn_deb_rub,
		turn_deb_val,
		turn_deb_total,
		turn_cre_rub,
		turn_cre_val,
		turn_cre_total,
		balance_out_rub,
		balance_out_val,
		balance_out_total
        
    )
    select
		i_ondate as from_date,
		v_to_date as to_date,
		l_a.chapter as chapter,
		l_a.ledger_account,
		a.char_type as characteristic,
		
		sum(case when a_b.on_date = prev_date and a.currency_code in ('810', '643') then a_b.balance_out_rub else 0 end) as balance_in_rub,
		sum(case when a_b.on_date = prev_date and a.currency_code not in ('810', '643') then a_b.balance_out else 0 end) as balance_in_val,
		sum(case when a_b.on_date = prev_date and a.currency_code in ('810', '643') then a_b.balance_out_rub else 0 end +
			case when a.currency_code not in ('810', '643') then a_b.balance_out else 0 end) as balance_in_total,
		
		
		sum(case when a.currency_code in ('810', '643') then coalesce(a_t.debet_amount_rub, 0) else 0 end) as turn_deb_rub,		
		sum(case when a.currency_code not in ('810', '643') then coalesce(a_t.debet_amount, 0) else 0 end) as turn_deb,		
		sum(case when a.currency_code in ('810', '643') then coalesce(a_t.debet_amount_rub, 0) else 0 end +
			case when a.currency_code not in ('810', '643') then coalesce(a_t.debet_amount, 0) else 0 end) as turn_deb_total,

		sum(case when a.currency_code in ('810', '643') then coalesce(a_t.credit_amount_rub, 0) else 0 end) as turn_cre_rub,
		sum(case when a.currency_code not in ('810', '643') then coalesce(a_t.credit_amount, 0) else 0 end) as turn_cre,
		sum(case when a.currency_code in ('810', '643') then coalesce(a_t.credit_amount_rub, 0) else 0 end +
			case when a.currency_code not in ('810', '643') then coalesce(a_t.credit_amount, 0) else 0 end) as turn_cre_total,

		sum(case when (a_b.on_date = v_to_date) and (a.currency_code in ('810', '643')) then coalesce(a_b.balance_out_rub, 0) else 0 end) as balance_out_rub,
		sum(case when (a_b.on_date = v_to_date) and (a.currency_code not in ('810', '643')) then coalesce(a_b.balance_out, 0) else 0 end) as balance_out_val,
		sum(case when (a_b.on_date = v_to_date) and (a.currency_code in ('810', '643')) then coalesce(a_b.balance_out_rub, 0) else 0 end +
			case when (a_b.on_date = v_to_date) and (a.currency_code not in ('810', '643')) then coalesce(a_b.balance_out, 0) else 0 end) as balance_out_total		
		
	from
		ds.md_account_d as a		
	left join
		ds.md_ledger_account_s l_a
		on l_a.ledger_account = cast(substring(cast(a.account_number as text) from 1 for 5) as integer)
	left join
		dm.dm_account_balance_f a_b
		on a_b.account_rk = a.account_rk			
	left join
		dm.dm_account_turnover_f a_t
		on a.account_rk = a_t.account_rk
	where a.data_actual_date = i_ondate
		and a.data_actual_end_date = v_to_date
	group by		
        l_a.chapter,
        l_a.ledger_account,
        a.char_type
	order by
		ledger_account;
end;
$$;

insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('fdm.dm_f101_round_f', current_timestamp, 'Создание процедуры для заполнения формы 101 завершено');

insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('fdm.dm_f101_round_f', current_timestamp, 'Вызов процедуры dm.fill_f101_round_f для января 2018');
	
call dm.fill_f101_round_f('2018-01-01');

select * from dm.dm_f101_round_f;

select * from dm.procedure_logs order by 1 desc;
