from .utilities.singleton import Singleton
from pathlib import Path
import re
import sys
import os
import datetime


class Storage(metaclass=Singleton):
    def __init__(self):

        self.database_path = self.__createDB()[1]
        print('aaa')




    def __createDB(self):

        db_name = ''

        if len(sys.argv) < 3:
            print("No DB name passed, a db with random name will be created in ..results/2-ds-results/")
            db_name = 'DataBase_' + str(datetime.datetime.now().timestamp()).replace('.','')

        else:

            db_name_check = bool(re.match('^[a-zA-Z0-9\-_]+$', str(sys.argv[2])))

            if not db_name_check:
                err_code = '1C'  # C stands for custom
                err_mess = 'INVALID NAME FOR A DB'
                err_details = 'please pass a name containing only letters/numbers/-/_  and no whitespace '
                raise ValueError(err_code, err_mess, err_details)

            db_name = str(sys.argv[2])

        script_root_path_str = str(Path(str(sys.argv[0])).absolute().parent.parent)
        db_dir_path = str(Path(script_root_path_str + "/results/2-ds-results/").absolute())
        try:
            os.makedirs(db_dir_path)
        except OSError as e:
            print("/results/2-ds-results/ " + " already exists... continuing ")
        print("directory initialized, creating DB")

        db_path = db_dir_path + '/' + db_name + '.db'

        check_var = True
        while check_var:
            try:
                Path(db_path).absolute().touch(exist_ok=False)
            except FileExistsError as e:
                print('E: ', e.strerror)
                db_path = db_dir_path + '/' + db_name + '_' + str(datetime.datetime.now().timestamp()).replace('.','') + '.db'
                continue
            check_var = False

        print("created DB: ",db_path)
        return 0,db_path
