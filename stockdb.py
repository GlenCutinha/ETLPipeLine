# Program name: stockdb.py
# Program description: Database interface for postgresql
# Created by: Glen Cutinha
# Created date: 04/02/2020
# Change history:
__author__ = 'glen'
import psycopg2
from config import Configuration
from utilities import CryptoPassword

class StockDb(object):
    def __init__(self):
        self.config = Configuration()
        self.crypt = CryptoPassword()
        self._get_db_conn_details()
        self.no_of_records = 0
        
    def dbconnect(self):
        try:
            self.dbconn = psycopg2.connect(host=self.__host, database=self.__dbname, user=self.__user, password=self.__password)
        except psycopg2.DatabaseError as e:
            raise Exception('Error connecting to database')
        self.cursor = self.dbconn.cursor()

    def _get_db_conn_details(self):
        self.__host = self.config.get_config_value('database', 'host')
        self.__user = self.config.get_config_value('database', 'user')
        self.__dbname = self.config.get_config_value('database', 'dbname')
        self.__password = self.crypt.decrypt_password(self.config.get_config_value('database', 'password'))
    
    def test_connection(self):
        sql_statement = '''SELECT VERSION()'''
        self.cursor.execute(sql_statement)
        print(self.cursor.fetchone())

    def fetch_records(self ):
        if self.no_of_records == 1:
            return self.cursor.fetchone()
        elif self.no_of_records == 0:
            return self.cursor.fetchall()
        else:
            return list(self.fetch_n_records(self.no_of_records))
    
    def fetch_n_records(self, no_of_records):
        while True:
            rows = self.cursor.fetchmany(no_of_records)
            if not rows:
                break
            for row in rows:
                yield row
    
    def mogrify_records(self, mogrify_str , sql_list):
        mogrified_str = b','.join(self.cursor.mogrify(mogrify_str, record) for record in sql_list)
        return mogrified_str.decode()


    def execute_statement(self, sql_tuple):
        try:
            statement_string = sql_tuple[0]
            statement_options = sql_tuple[1]
            self.no_of_records = sql_tuple[2]
            # print(statement_string, statement_options)
            if statement_options:
                self.cursor.execute(statement_string, statement_options)
            else:
                self.cursor.execute(statement_string)
        except psycopg2.DatabaseError as e:
            raise Exception('Error while executing sql statement =>' + e.pgcode + ' ' + e.pgerror )

    def dbdisconnect(self):
        self.dbconn.close()
    
    def commit_transaction(self):
        self.dbconn.commit()
    
    def rollback_transaction(self):
        self.dbconn.rollback()

if __name__ == "__main__":
    stock = StockDb()
    stock.test_connection()