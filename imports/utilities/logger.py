from .singleton import Singleton
from .outputDirectoryManager import OutputDirectoryManager


class Logger(metaclass=Singleton):

    def __init__(self):
        output_dir = OutputDirectoryManager().getOutputDir()
        self.__log_file_path = output_dir + '/' + 'log.txt'
        self.__log_fd = open(self.__log_file_path,'a')

    def log(self,tag,value):
        #todo str() può lanciare eccezioni
        tag = str(tag).rstrip()
        value = str(value).rstrip()
        msg = tag + '; ' + value + '\n'
        self.__log_fd.write(msg)