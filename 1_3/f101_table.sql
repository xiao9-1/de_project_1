-- создание таблицы для формы 101;

insert into dm.procedure_logs (log_name, log_time, log_message)
    values ('fdm.dm_f101_round_f', current_timestamp, 'Создание таблицы для формы 101');

create table if not exists dm.dm_f101_round_f (
	from_date date,
	to_date date,
	chapter char(1),
	ledger_account char(5),
	characteristic char(1),
	balance_in_rub numeric(23,8),
	r_balance_in_rub numeric(23,8),
	balance_in_val numeric(23,8),
	r_balance_in_val numeric(23,8),
	balance_in_total numeric(23,8),
	r_balance_in_total numeric(23,8),
	turn_deb_rub numeric(23,8),
	r_turn_deb_rub numeric(23,8),
	turn_deb_val numeric(23,8),
	r_turn_deb_val numeric(23,8),
	turn_deb_total numeric(23,8),
	r_turn_deb_total numeric(23,8),
	turn_cre_rub numeric(23,8),
	r_turn_cre_rub numeric(23,8),
	turn_cre_val numeric(23,8),
	r_turn_cre_val numeric(23,8),
	turn_cre_total numeric(23,8),
	r_turn_cre_total numeric(23,8),
	balance_out_rub numeric(23,8),
	r_balance_out_rub numeric(23,8),
	balance_out_val numeric(23,8),
	r_balance_out_val numeric(23,8),
	balance_out_total numeric(23,8),
	r_balance_out_total numeric(23,8)
);
