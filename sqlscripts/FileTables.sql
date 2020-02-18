create table ft_nifty50
(
    stock_date  varchar(50),
    symbol varchar(50),
    series varchar(50),
    prev_close varchar(50),
    open varchar(50),
    high varchar(50),
    low varchar(50),
    last varchar(50),
    close varchar(50),
    vwap varchar(50),
    volume varchar(50),
    turnover varchar(50),
    trades varchar(50),
    deliverable_volume varchar(50),
    deliverble varchar(50)
);

create table ft_nifty50_meta
(
    company_name varchar(100),
    industry varchar(50),
    symbol varchar(50),
    series varchar(50),
    ISIN varchar(50)
);

create table nifty50_file_master 
(
    fileid integer,
    filename varchar(100),
    processing_start_date timestamp,
    processing_end_date timestamp,
    sub_files_count integer,
    expct_sub_file_count integer,
    no_records_processed integer,
    file_processed varchar(1),
    error_descrption varchar(100)
);

create table nifty50_sub_file_master 
(
    fileid integer,
    file_name varchar(100),
    sub_filename varchar(100),
    processing_start_date timestamp,
    processing_end_date timestamp,
    no_records_processed integer,
    file_processed varchar(1),
    error_descrption varchar(100)
);