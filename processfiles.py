# Program name: processfiles.py
# Program description: File processing logic for nifty 50 files
# Created by: Glen Cutinha
# Created date: 03/02/2020
# Change history:
__author__ = 'glen'
import os.path
import zipfile
from config import Configuration
from unzipfiles import UnzipFiles
from loadfiles import LoadManager


class ProcessNiftyFiles(object):
    def __init__(self):
        self.config = Configuration()
    
    def unzipfiles(self):
        self.zip_filepath = self.config.get_config_value('fileprocessing', 'zip_filepath')
        self.unzip_path = self.config.get_config_value('fileprocessing', 'filepath')
        unzip_obj = UnzipFiles(self.zip_filepath, self.unzip_path)
        unzip_obj.extract_zip()
        unzip_obj.update_file_master()
    
    def loadfiles(self):
        self.queue_length = self.config.get_config_value('loadmanager', 'queue_length')
        load_file_obj = LoadManager(self.unzip_path, self.queue_length)
        load_file_obj.run()


if __name__ == "__main__":
    process_obj = ProcessNiftyFiles()
    process_obj.unzipfiles()