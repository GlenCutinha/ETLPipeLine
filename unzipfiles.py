# Program name: unzipfiles.py
# Program description: unzip the zip archive and move files to processing folder
# Created by: Glen cutinha
# Created date: 07/02/2020
# Change history: 
__author__ = 'glen'
from os import sep
import os.path
from queries import get_filename, get_fileid, insert_into_subfile_master, update_file_master
import zipfile
from utilities import getcurrenttime, generate_file_path

class UnzipFiles(object):
    def __init__(self, zip_path, unzip_path):
        self.zip_path = generate_file_path(zip_path)
        self.unzip_path = generate_file_path(unzip_path)
        self._check_path_exists()
        self.filename = get_filename('N')
        self.error_flag = 'N'
        self.error_msg = ''
    
    def _check_path_exists(self):
        if not os.path.isdir(self.zip_path):
            self.error_flag = 'E'
            self.error_msg = 'Zip directory not found'
        if not os.path.isdir(self.unzip_path):
            self.error_flag = 'E'
            self.error_msg = 'Processing directory not found'
    
    def extract_zip(self):
        try:
            self.filename = self.filename[0]
            zip_file_loc = self.zip_path  + self.filename
            with zipfile.ZipFile( zip_file_loc, 'r') as zip:
                zip.extractall(self.unzip_path)
                self._record_data_file_sub_master(zip)
        except Exception as e:
            self.error_flag = 'E'
            self.error_msg = 'Error in extract_zip method ' + str(e)
    
    def _record_data_file_sub_master(self, obj_zip):
        files_name_list = obj_zip.namelist()
        file_id = get_fileid(self.filename)
        file_id = file_id[0]
        sub_file_master_list = []
        for files in files_name_list:
            sub_file_master_list.append((str(file_id), self.filename, files, 'N'))
        insert_into_subfile_master(sub_file_master_list)
    
    def update_file_master(self):
        update_file_master('N', self.filename, 'P')