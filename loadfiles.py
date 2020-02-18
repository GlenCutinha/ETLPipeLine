# Program name: loadfiles.py
# Program Description: load unziped files into database using multiprocessing
# Created by : Glen Cutinha
# Created date: 14/02/2020
__author__ = 'glen'

import multiprocessing
from queries import get_filename, get_sub_file_names, update_sub_file_master, insert_sub_files
from time import sleep
import csv
import os.path
import sys
from datetime import datetime

class LoadFiles(object):

    def __init__(self, loadpath, filename):
        self.path = loadpath
        self.filename = filename
        self.error_flag = 'N'
        self.error_msg = ''
        self.row_length = 0
        self.sub_file_records_list = []
        self._process_sub_file()
    
    def _process_sub_file(self):
        # update_sub_file_master('N', self.filename, 'P')
        self._create_sub_file_rec_list()
        self._insert_sub_file_list()
    
    def _create_sub_file_rec_list(self):
        file_path = self.path + os.path.sep + self.filename        
        try:
            with open(file_path, 'rt') as sub_file:
                csv_dict_reader = csv.reader(sub_file)
                next(csv_dict_reader)  # to skip the first line
                for row in csv_dict_reader:
                    self.sub_file_records_list.append(tuple(row))
            self.row_length = len(self.sub_file_records_list[0])
        except Exception as e:
            self.error_flag = 'E'
            self.error_msg = 'Error in _create_sub_file_rec_list method ' + str(e)
    
    def _insert_sub_file_list(self):
        insert_sub_files(self.filename, self.sub_file_records_list, self.row_length)
            

class LoadFileManager(object):
    '''
     class used for parallel execution of file loading

    '''
    
    def __init__(self, loadpath, queue_length):
        self.loadpath = loadpath
        self.load_queue_length = queue_length
        self._job_queue = []
        self.file_indices = 0
        self._get_file_data()
    
    def _get_file_data(self):
        main_file_name = 'nifty50-stock-market-data.zip'#get_filename('P')
        self.sub_filename_list = get_sub_file_names(main_file_name, 'N')
        self._clean_sub_file_list()

    def _clean_sub_file_list(self):
        temp_list = []
        for rec in self.sub_filename_list:
            temp_list.append(rec[0])
        self.sub_filename_list = temp_list
    
    def add_exec_jobs(self):
        for i in range(self.file_indices, len(self.sub_filename_list) ):
            if len(self._job_queue) < self.load_queue_length:
                fileload_process =  multiprocessing.Process(target=LoadFiles, args=(self.loadpath, self.sub_filename_list[i],))
                self._job_queue.append(fileload_process)
                fileload_process.run()
            else:
                self.file_indices = i
                if self.file_indices == len(self.sub_filename_list) - 1:
                    break
                self._monitor_jobs()
    
    def _monitor_jobs(self):
        while True:
            if len(self._job_queue) < self.load_queue_length:
                break
            else:
                self._job_queue = [ x for x in self._job_queue if x.is_alive()] 
                sleep(1)


if __name__ == "__main__":
    l = LoadFileManager('files\processing', 5)
    start_time = datetime.now()
    l.add_exec_jobs()
    end_time = datetime.now()
    print(end_time - start_time)
