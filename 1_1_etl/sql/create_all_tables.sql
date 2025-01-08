-- Удаление таблицы DS.FT_BALANCE_F, если она существует
DROP TABLE IF EXISTS ds.ft_balance_f;

-- Создание таблицы DS.FT_BALANCE_F с PK
CREATE TABLE ds.ft_balance_f (
    on_date DATE NOT NULL,
    account_rk INTEGER NOT NULL,
    currency_rk INTEGER,
    balance_out FLOAT,
    PRIMARY KEY (on_date, account_rk) -- Первичный ключ
);

-- Удаление таблицы DS.FT_POSTING_F, если она существует
DROP TABLE IF EXISTS ds.ft_posting_f;

-- Создание таблицы DS.FT_POSTING_F (без PK)
CREATE TABLE ds.ft_posting_f (
    oper_date DATE NOT NULL,
    credit_account_rk INTEGER NOT NULL,
    debet_account_rk INTEGER NOT NULL,
    credit_amount FLOAT,
    debet_amount FLOAT
);

-- Удаление таблицы DS.MD_ACCOUNT_D, если она существует
DROP TABLE IF EXISTS ds.md_account_d;

-- Создание таблицы DS.MD_ACCOUNT_D с PK
CREATE TABLE ds.md_account_d (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    account_rk INTEGER NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk INTEGER NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    PRIMARY KEY (data_actual_date, account_rk) -- Первичный ключ
);

-- Удаление таблицы DS.MD_CURRENCY_D, если она существует
DROP TABLE IF EXISTS ds.md_currency_d;

-- Создание таблицы DS.MD_CURRENCY_D с PK
CREATE TABLE ds.md_currency_d (
    currency_rk INTEGER NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3),
    PRIMARY KEY (currency_rk, data_actual_date) -- Первичный ключ
);

-- Удаление таблицы DS.MD_EXCHANGE_RATE_D, если она существует
DROP TABLE IF EXISTS ds.md_exchange_rate_d;

-- Создание таблицы DS.MD_EXCHANGE_RATE_D с PK
CREATE TABLE ds.md_exchange_rate_d (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk INTEGER NOT NULL,
    reduced_cource FLOAT,
    code_iso_num VARCHAR(3),
    PRIMARY KEY (data_actual_date, currency_rk) -- Первичный ключ
);

-- Удаление таблицы DS.MD_LEDGER_ACCOUNT_S, если она существует
DROP TABLE IF EXISTS ds.md_ledger_account_s;

-- Создание таблицы DS.MD_LEDGER_ACCOUNT_S с PK
CREATE TABLE ds.md_ledger_account_s (
    chapter CHAR(1) NOT NULL,
    chapter_name VARCHAR(16),
    section_number INTEGER,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(47),
    ledger_account INTEGER NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    is_resident INTEGER,
    is_reserve INTEGER,
    is_reserved INTEGER,
    is_loan INTEGER,
    is_reserved_assets INTEGER,
    is_overdue INTEGER,
    is_interest INTEGER,
    pair_account VARCHAR(5),
    start_date DATE NOT NULL,
    end_date DATE,
    is_rub_only INTEGER,
    min_term INTEGER,
    min_term_measure VARCHAR(1),
    max_term INTEGER,
    max_term_measure VARCHAR(1),
    ledger_acc_full_name_translit VARCHAR(1),
    is_revaluation VARCHAR(1),
    is_correct VARCHAR(1),
    PRIMARY KEY (ledger_account, start_date) -- Первичный ключ
);

