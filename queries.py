# Program name: queries.py
# Program description: queries to process records using function
# Created by: Glen Cutinha
# Created date: 02/07/2020
# Change history:
__author__ = 'glen'
from stockdb import StockDb

db_obj = StockDb()

def dec_select_executor(sql_stmt):
    def querywrapper(*args, **kwargs):
        db_obj.dbconnect()
        if args or kwargs:
            db_obj.execute_statement(sql_stmt(*args, ** kwargs))
        else:
            db_obj.execute_statement(sql_stmt())
        query_result = db_obj.fetch_records()
        db_obj.dbdisconnect()
        return query_result
    return querywrapper

def dec_dml_executor(sql_stmt):
    def querywrapper(*args, **kwargs):
        db_obj.dbconnect()
        if args or kwargs:
            db_obj.execute_statement(sql_stmt(*args, ** kwargs))
        else:
            db_obj.execute_statement(sql_stmt())
        db_obj.commit_transaction()
        db_obj.dbdisconnect()
    return querywrapper



@dec_select_executor
def get_filename(file_processed):
    sql_stmt = '''SELECT filename FROM nifty50_file_master WHERE  FILE_PROCESSED = %s'''
    stmt_option =  (file_processed,)
    no_records = 1
    return (sql_stmt, stmt_option, no_records)

@dec_select_executor
def get_fileid(file_name):
    sql_stmt = '''SELECT fileid FROM nifty50_file_master WHERE  FILENAME = %s'''
    stmt_option =  (file_name,)
    no_records = 1
    return (sql_stmt, stmt_option, no_records)

@dec_dml_executor
def insert_into_subfile_master(values_list):
    sql_stmt = '''INSERT INTO nifty50_sub_file_master(fileid, file_name, sub_filename, file_processed)
                  VALUES ''' + db_obj.mogrify_records("(cast(%s  as integer), %s, %s, %s)",values_list)
    stmt_option = None
    no_records = 0
    return (sql_stmt, stmt_option, no_records)    

@dec_select_executor
def get_sub_file_names(filename, processed_state):
    sql_stmt = '''SELECT sub_filename FROM nifty50_sub_file_master WHERE  file_name = %s and file_processed = %s''' 
    stmt_option = (filename,processed_state )
    no_records = 0
    return (sql_stmt, stmt_option, no_records)

@dec_dml_executor
def update_file_master(current_stage, filename, prev_stage):
    sql_stmt = ''' UPDATE  nifty50_file_master SET file_processed = %s WHERE filename = %s and file_processed = %s '''
    stmt_option = (prev_stage, filename, prev_stage)
    no_records = 0 
    return (sql_stmt, stmt_option, no_records)

@dec_dml_executor
def update_sub_file_master(current_stage, filename, prev_stage):
    sql_stmt = ''' UPDATE  nifty50_sub_file_master SET file_processed = %s WHERE filename = %s and file_processed = %s '''
    stmt_option = (prev_stage, filename, prev_stage)
    no_records = 0 
    return (sql_stmt, stmt_option, no_records)

@dec_dml_executor
def insert_sub_files(filename, values_list, row_length):
    if 'metadata' in  filename:
        sql_str = ''' INSERT INTO ft_nifty50_meta VALUES '''
    else:
        sql_str = ''' INSERT INTO ft_nifty50 VALUES '''
    mogrify_str_list = ['%s'] * row_length
    morgify_str = '(' + ','.join(mogrify_str_list) + ')'
    sql_stmt = sql_str + db_obj.mogrify_records(morgify_str, values_list)
    stmt_option = None
    no_records = 0
    return (sql_stmt, stmt_option, no_records)   


if __name__ == "__main__":
    pass