# Program name: config.py
# Program description: Get config value from ini file
# Program created by: Glen Cutinha
# Program craeted date: 03/02/2020
# Change history
__author__ = 'glen'

from configparser import ConfigParser
from platform import os

config_file = 'nifty50.ini'
parser = ConfigParser()
parser.read(config_file)

class Configuration(object):
    def __init__(self):
        self.config_file = 'nifty50.ini'
        self.parser = ConfigParser()
        self.parser.read(self.config_file)

    def get_config_value(self, p_section, p_option):
        if self.parser.has_section(p_section) and self.parser.has_option(p_section,p_option):
            return self.parser.get(p_section, p_option)
        else:
            raise Exception('Invalid section = ' + p_section + ' or option = ' \
                            + p_option + ' plese refer ' + self.config_file +' config file')

    def set_config_value(self, p_section, p_option, p_value):
        if not self.parser.has_section(p_section):
            try:
                self.parser.add_section(p_section)
            except Exception:
                raise Exception('Error while adding section to config file =>' + self.config_file)
        
        try:
                self.parser.set(p_section, p_option, p_value)
                with open(self.config_file,'w') as configfile:
                    self.parser.write(configfile)
        except Exception:
                raise Exception('Error while adding option and value to config file')
    
        
# if __name__ == '__main__':
#     c = Configuration()
#     print(c.get_config_value('fileprocessing','zip_filepath'))
#     print(get_config_value('fileprocesing','zip_filepath'))
#     print(get_config_value('fileprocessing','zip_filepah'))