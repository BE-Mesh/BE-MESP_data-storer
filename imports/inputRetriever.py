from .utilities.singleton import Singleton
from pathlib import Path
import sys
import re
from os import walk



class InputRetriever(metaclass=Singleton):
    def __init__(self):

        self.__input_dir_path = self.__input_dir_path_validator()[1]
        self.__input_files_list = self.__retrieve_input_files_list()[1]



    def __input_dir_path_validator(self):


        if len(sys.argv) < 2:
            err_code = '2C'  # C stands for custom
            err_mess = 'MISSING INPUT DIRECTORY NAME'
            err_details = 'please pass the name of the directory containing the input files'
            raise ValueError(err_code, err_mess, err_details)

        input_dir_name_check = bool(re.match('^[a-zA-Z0-9\-_]+$', str(sys.argv[1])))

        if not input_dir_name_check:
            err_code = '3C'  # C stands for custom
            err_mess = 'INVALID NAME FOR AN INPUT DIR'
            err_details = 'please pass a name containing only letters/numbers/-/_  and no whitespace '
            raise ValueError(err_code, err_mess, err_details)



        script_root_path_str = str(Path(str(sys.argv[0])).absolute().parent.parent)
        input_dir_path = Path(script_root_path_str + "/results/1-dc-results/" + str(sys.argv[1])).absolute()

        if not input_dir_path.is_dir():
            err_code = '4C'  # C stands for custom
            err_mess = 'NO DIRECTORY FOUND '
            err_details = 'no directory found at ' + str(input_dir_path)
            raise ValueError(err_code, err_mess, err_details)

        return 0, str(input_dir_path)

    def __retrieve_input_files_list(self):

        f_l_t = []
        for (dirpath, dirnames, filenames) in walk(self.__input_dir_path):
            f_l_t.extend(filenames)
            break

        loc_input_file_list = []

        for file_entry in f_l_t:
            name_check = bool(re.match('^(?!\.).*(\.csv)$', file_entry))
            if name_check:
                loc_input_file_list.append(self.__input_dir_path + '/' + file_entry)

        print('__input_dir_path ',self.__input_dir_path)
        print("loc_input_file_list ",loc_input_file_list)


        return 0,loc_input_file_list


    def getInputDirPath(self):
        return self.__input_dir_path


    def getInputFilesList(self):
        return self.__input_files_list