select * from nifty50_sub_file_master;

select *
from pg_stat_activity
where datname = 'stockdb';


drop table nifty50_file_master ;
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

select * from nifty50_file_master;

insert into nifty50_file_master (fileid, filename, file_processed) values (1, 'nifty50-stock-market-data.zip', 'N');
commit;

select * from nifty50_file_master;
select * from nifty50_sub_file_master;