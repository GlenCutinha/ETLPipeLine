from cryptography.fernet import Fernet
from config import Configuration
import os.path
import platform
from datetime import datetime


class CryptoPassword(object):
    def __init__(self):
        self.config = Configuration()
        self.key = self._get_cipher_key()
        self.cipher = Fernet(self.key)
        
    def _get_cipher_key(self):
        keydir = self.config.get_config_value('key', 'keyfile')     
        if not os.path.isdir(keydir):
            raise Exception('Key file directory not found')
        keyfile =  keydir + os.path.sep +'key.txt'
        if os.path.isfile(keyfile):
            with open(keyfile,'rb') as cipherkey:
                cipherkeylist = cipherkey.readlines()
                if cipherkeylist:
                    return cipherkeylist[0]
        else:
            with open(keyfile,'wb') as cipherkey:
                key = Fernet.generate_key()
                cipherkey.write(key)
                return key
    
    def encrypt_password(self):
        get_password = self.config.get_config_value('database', 'password')
        if not get_password:
            password = input('Enter the password:')
            password = password.encode()
            enc_password = self.cipher.encrypt(password)
            enc_password = enc_password.decode()
            self.config.set_config_value('database', 'password', enc_password)
    
    def decrypt_password(self, password):
        get_password = password
        if not get_password:
            raise Exception("Set the password first")
        else:
            password = self.cipher.decrypt(get_password.encode())
            return password.decode()

def getcurrenttime():
    current_dt = str(datetime.now())
    current_dt = current_dt[:current_dt.find('.')]
    print(current_dt)


def generate_file_path(file_str):
    file_path_list = file_str.split(',')
    file_path = '' + os.path.sep
    return file_path.join(file_path_list) + os.path.sep

if __name__ == '__main__':
    # crypt = CryptoPassword()
    # print(crypt.key)
    # crypt.encrypt_password()
    # print(crypt.decrypt_password())
    getcurrenttime()