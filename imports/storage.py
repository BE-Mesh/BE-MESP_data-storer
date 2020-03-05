from .utilities.singleton import Singleton
from .utilities.databaseManager import DatabaseManager
from pathlib import Path
import re
import sys
import os
import datetime


class Storage(metaclass=Singleton):
    def __init__(self):

        self.__checkValidityDIRName()
        self.__directory_path = self.__initializeOutputDir()[1]

        self.__database_path = self.__createDB()[1]
        self.__DBM = DatabaseManager(self.__database_path)
        self.__initializeDBstructure()

    def __checkValidityDIRName(self):

        if len(sys.argv) < 4:
            err_code = '1C'  # C stands for custom
            err_mess = 'NO OUTPUT DIRECTORY VALUES PASSED AS ARGUMENT'
            err_details = 'please pass the name of the output case directory and the name of subcase subdirectory'
            raise ValueError(err_code, err_mess, err_details)

        case_dir_name_check = bool(re.match('^[a-zA-Z0-9\-_]+$', str(sys.argv[2])))
        subcase_dir_name_check = bool(re.match('^[a-zA-Z0-9\-_]+$', str(sys.argv[3])))

        if not case_dir_name_check or not subcase_dir_name_check:
            err_code = '2C'  # C stands for custom
            err_mess = 'INVALID NAME FOR A DIRECTORY'
            err_details = 'please pass a name containing only letters/numbers/-/_  and no whitespace '
            raise ValueError(err_code, err_mess, err_details)

    def __initializeOutputDir(self):

        script_root_path_str = str(Path(str(sys.argv[0])).absolute().parent.parent)
        dir_path = Path(script_root_path_str + "/results/2-ds-results/" + str(sys.argv[2])
                        +'/' + str(sys.argv[3])).absolute()
        print("Checking if ./results/2-ds-results/" + str(sys.argv[2]) +'/' + str(sys.argv[3]) + " exists...")
        try:
            os.makedirs(dir_path)
        except OSError as e:
            print("/results/2-ds-results/" + str(sys.argv[2]) +'/' + str(sys.argv[3]) + " already exists, continuing... ")

        print("DIR initialized")
        return 0,str(dir_path)



    def __createDB(self):

        db_name = ''

        if len(sys.argv) < 5:
            print("No DB name passed, a db with random name will be created in ..results/2-ds-results/")
            db_name = 'DataBase_' + str(datetime.datetime.now().timestamp()).replace('.','')

        else:

            db_name_check = bool(re.match('^[a-zA-Z0-9\-_]+$', str(sys.argv[4])))

            if not db_name_check:
                err_code = '2C'  # C stands for custom
                err_mess = 'INVALID NAME FOR A DB'
                err_details = 'please pass a name containing only letters/numbers/-/_  and no whitespace '
                raise ValueError(err_code, err_mess, err_details)

            db_name = str(sys.argv[4])

        db_path = self.__directory_path + '/' + db_name + '.db'

        check_var = True
        while check_var:
            try:
                Path(db_path).absolute().touch(exist_ok=False)
            except FileExistsError as e:
                print('E: ', e.strerror)
                db_path = self.__directory_path + '/' + db_name + '_' + str(datetime.datetime.now().timestamp()).replace('.','') + '.db'
                continue
            check_var = False

        print("created DB: ",db_path)
        return 0,db_path



    def __initializeDBstructure(self):
        print("Initializing DB...")
        sql_create_table_devices = """ CREATE TABLE IF NOT EXISTS devices (
                                            BLE_address text PRIMARY KEY
                                        ); """

        sql_create_table_events = """CREATE TABLE IF NOT EXISTS events (
                                        event_id integer PRIMARY KEY AUTOINCREMENT,
                                        submitter_id text NOT NULL,
                                        timestamp integer NOT NULL,
                                        type integer NOT NULL,
                                        FOREIGN KEY (submitter_id) REFERENCES devices (BLE_address)
                                    );"""

        sql_create_table_TE_message_sent = """CREATE TABLE IF NOT EXISTS typeEvent_message_sent (
                                        id integer PRIMARY KEY AUTOINCREMENT,
                                        event_id text NOT NULL UNIQUE,
                                        sender_id text NOT NULL,
                                        receiver_id text NOT NULL,
                                        next_hop_id text NOT NULL,
                                        message_type text NOT NULL,
                                        sequence_number integer,
                                        payload text NOT NULL,
                                        FOREIGN KEY (event_id) REFERENCES events (event_id)
                                    );"""

        sql_create_table_TE_message_received = """CREATE TABLE IF NOT EXISTS typeEvent_message_received (
                                        id integer PRIMARY KEY AUTOINCREMENT,
                                        event_id text NOT NULL UNIQUE,
                                        sender_id text NOT NULL,
                                        receiver_id text NOT NULL,
                                        prev_hop_id text NOT NULL,
                                        message_type text NOT NULL,
                                        sequence_number integer,
                                        payload text NOT NULL,
                                        FOREIGN KEY (event_id) REFERENCES events (event_id)
                                    );"""

        sql_create_table_TE_incoming_connection_attempts = """CREATE TABLE IF NOT EXISTS typeEvent_incoming_connection_attempts (
                                                id integer PRIMARY KEY AUTOINCREMENT,
                                                event_id text NOT NULL UNIQUE,
                                                requester_id text NOT NULL,
                                                FOREIGN KEY (event_id) REFERENCES events (event_id)
                                            );"""

        sql_create_table_TE_outgoing_connection_attempts = """CREATE TABLE IF NOT EXISTS typeEvent_outgoing_connection_attempts (
                                                id integer PRIMARY KEY AUTOINCREMENT,
                                                event_id text NOT NULL UNIQUE,
                                                target_id text NOT NULL,
                                                FOREIGN KEY (event_id) REFERENCES events (event_id)
                                            );"""

        sql_create_table_TE_connection_attempts_outcomes = """CREATE TABLE IF NOT EXISTS typeEvent_connection_attempts_outcomes (
                                                id integer PRIMARY KEY AUTOINCREMENT,
                                                event_id text NOT NULL UNIQUE,
                                                requester_id text NOT NULL,
                                                target_id text NOT NULL,
                                                outcome text NOT NULL,
                                                FOREIGN KEY (event_id) REFERENCES events (event_id)
                                            );"""

        sql_create_table_TE_device_up = """CREATE TABLE IF NOT EXISTS typeEvent_device_up (
                                                id integer PRIMARY KEY AUTOINCREMENT,
                                                event_id text NOT NULL UNIQUE,
                                                FOREIGN KEY (event_id) REFERENCES events (event_id)
                                            );"""

        sql_create_table_TE_assume_role = """CREATE TABLE IF NOT EXISTS typeEvent_assume_role (
                                                        id integer PRIMARY KEY AUTOINCREMENT,
                                                        event_id text NOT NULL UNIQUE,
                                                        role text NOT NULL,
                                                        FOREIGN KEY (event_id) REFERENCES events (event_id)
                                                    );"""

        sql_create_table_TE_scan = """CREATE TABLE IF NOT EXISTS typeEvent_scan (
                                                        id integer PRIMARY KEY AUTOINCREMENT,
                                                        event_id text NOT NULL UNIQUE,
                                                        start_or_end text NOT NULL,
                                                        FOREIGN KEY (event_id) REFERENCES events (event_id)
                                                    );"""


        self.__DBM.createTable(sql_create_table_devices)
        self.__DBM.createTable(sql_create_table_events)
        self.__DBM.createTable(sql_create_table_TE_message_sent)
        self.__DBM.createTable(sql_create_table_TE_message_received)
        self.__DBM.createTable(sql_create_table_TE_incoming_connection_attempts)
        self.__DBM.createTable(sql_create_table_TE_outgoing_connection_attempts)
        self.__DBM.createTable(sql_create_table_TE_connection_attempts_outcomes)
        self.__DBM.createTable(sql_create_table_TE_device_up)
        self.__DBM.createTable(sql_create_table_TE_assume_role)
        self.__DBM.createTable(sql_create_table_TE_scan)

        return 0,None

    #TODO: implementare il salvataggio su tabella per ogni evento
    def storeMessageSentEvent(self,ts,submitter_id,sender,receiver,next_hop,message_type,payload,sequence_number):
        event_type = 0
        print ("Storing Message-Sent Event...")

        fb = self.__registerSubmitterDeviceOnDB(submitter_id)
        if fb[0] != 0:
            return fb


        fb = self.__DBM.storeTableEntry('events', [submitter_id,ts,event_type])
        if fb[0] != 0:
            return fb[0],'Error while storing Message-Sent Event:  ' + fb[1]

        res = self.__DBM.getTuple('events', submitter_id=submitter_id,timestamp=ts,type=event_type)

        if res[0] > 0:
            return res[0],'Error while storing Message-Sent Event: ' + res[1]

        if (res[0] == 0 and (res[1] is None)):
            raise Exception('entry not present on DB while previously stored, EXIT')

        event_id = res[1][0]
        fb = self.__DBM.storeTableEntry('typeEvent_message_sent',
                                        [event_id,sender,receiver,next_hop,message_type,sequence_number,payload])

        if fb[0] != 0:
            print('ERROR: ', fb[1])
            raise Exception('Impossible to store entry in typeEvent_message_sent table, DB integrity compromised, EXIT')

        return 0,None

    def storeMessageRcvEvent(self,ts,submitter_id,sender,receiver,prev_hop,message_type,payload,sequence_number):
        event_type = 1
        print ("Storing Message-Received Event...")

        fb = self.__registerSubmitterDeviceOnDB(submitter_id)
        if fb[0] != 0:
            return fb

        fb = self.__DBM.storeTableEntry('events', [submitter_id,ts,event_type])
        if fb[0] != 0:
            return fb[0],'Error while storing Message-Received Event:  ' + fb[1]

        res = self.__DBM.getTuple('events', submitter_id=submitter_id, timestamp=ts, type=event_type)

        if res[0] > 0:
            return res[0], 'Error while storing Message-Received Event: ' + res[1]

        if (res[0] == 0 and (res[1] is None)):
            raise Exception('entry not present on DB while previously stored, EXIT')

        event_id = res[1][0]
        fb = self.__DBM.storeTableEntry('typeEvent_message_received',
                                        [event_id, sender, receiver, prev_hop, message_type,sequence_number, payload])

        if fb[0] != 0:
            print('ERROR: ',fb[1])
            raise Exception('Impossible to store entry in typeEvent_message_received table, DB integrity compromised, EXIT')

        return 0,None

    def storeOutgoingConnectionAttemptEvent(self,ts,submitter_id,connect_to):
        event_type = 2
        print ("Storing Outgoing ConnectionAttempt Event...")

        fb = self.__registerSubmitterDeviceOnDB(submitter_id)
        if fb[0] != 0:
            return fb

        fb = self.__DBM.storeTableEntry('events', [submitter_id, ts, event_type])
        if fb[0] != 0:
            return fb[0], 'Error while storing Outgoing ConnectionAttempt Event:  ' + fb[1]

        res = self.__DBM.getTuple('events', submitter_id=submitter_id, timestamp=ts, type=event_type)

        if res[0] > 0:
            return res[0], 'Error while storing Outgoing ConnectionAttempt Event: ' + res[1]

        if (res[0] == 0 and (res[1] is None)):
            raise Exception('entry not present on DB while previously stored, EXIT')

        event_id = res[1][0]
        fb = self.__DBM.storeTableEntry('typeEvent_outgoing_connection_attempts',
                                        [event_id,connect_to])

        if fb[0] != 0:
            print('ERROR: ', fb[1])
            raise Exception('Impossible to store entry in typeEvent_outgoing_connection_attempts table, DB integrity compromised, EXIT')

        return 0,None

    def storeIncomingConnectionAttemptEvent(self,ts,submitter_id,connect_from):
        event_type = 3
        print ("Storing Incoming ConnectionAttempt Event...")
        fb = self.__registerSubmitterDeviceOnDB(submitter_id)
        if fb[0] != 0:
            return fb

        fb = self.__DBM.storeTableEntry('events', [submitter_id, ts, event_type])
        if fb[0] != 0:
            return fb[0], 'Error while storing Incoming ConnectionAttempt Event:  ' + fb[1]

        res = self.__DBM.getTuple('events', submitter_id=submitter_id, timestamp=ts, type=event_type)

        if res[0] > 0:
            return res[0], 'Error while storing Incoming ConnectionAttempt Event: ' + res[1]

        if (res[0] == 0 and (res[1] is None)):
            raise Exception('entry not present on DB while previously stored, EXIT')

        event_id = res[1][0]
        fb = self.__DBM.storeTableEntry('typeEvent_incoming_connection_attempts',
                                        [event_id, connect_from])

        if fb[0] != 0:
            print('ERROR: ', fb[1])
            raise Exception('Impossible to store entry in typeEvent_incoming_connection_attempts table, DB integrity compromised, EXIT')

        return 0, None

    def storeConnectionAttemptResultEvent(self,ts,submitter_id,connect_from,connect_to,outcome):
        event_type = 4
        print ("Storing ConnectionAttemptResult Event with result %s..." % outcome)
        fb = self.__registerSubmitterDeviceOnDB(submitter_id)
        if fb[0] != 0:
            return fb

        fb = self.__DBM.storeTableEntry('events', [submitter_id, ts, event_type])
        if fb[0] != 0:
            return fb[0], 'Error while storing ConnectionAttemptResult Event:  ' + fb[1]

        res = self.__DBM.getTuple('events', submitter_id=submitter_id, timestamp=ts, type=event_type)

        if res[0] > 0:
            return res[0], 'Error while storing ConnectionAttemptResult Event: ' + res[1]

        if (res[0] == 0 and (res[1] is None)):
            raise Exception('entry not present on DB while previously stored, EXIT')

        event_id = res[1][0]
        fb = self.__DBM.storeTableEntry('typeEvent_connection_attempts_outcomes',
                                        [event_id, connect_from,connect_to,outcome])

        if fb[0] != 0:
            print('ERROR: ', fb[1])
            raise Exception('Impossible to store entry in typeEvent_connection_attempts_outcomes table, DB integrity compromised, EXIT')

        return 0, None

    def storeDeviceUpEvent(self,ts,submitter_id):
        event_type = 5
        print ("Storing Device Up Event...")
        fb = self.__registerSubmitterDeviceOnDB(submitter_id)
        if fb[0] != 0:
            return fb

        fb = self.__DBM.storeTableEntry('events', [submitter_id, ts, event_type])
        if fb[0] != 0:
            return fb[0], 'Error while storing Device Up Event:  ' + fb[1]

        res = self.__DBM.getTuple('events', submitter_id=submitter_id, timestamp=ts, type=event_type)

        if res[0] > 0:
            return res[0], 'Error while storing Device Up Event: ' + res[1]

        if (res[0] == 0 and (res[1] is None)):
            raise Exception('entry not present on DB while previously stored, EXIT')

        event_id = res[1][0]
        fb = self.__DBM.storeTableEntry('typeEvent_device_up',
                                        [event_id])

        if fb[0] != 0:
            print('ERROR: ', fb[1])
            raise Exception('Impossible to store entry in typeEvent_device_up table, DB integrity compromised, EXIT')

        return 0, None

    def storeAssumeRoleEvent(self,ts,submitter_id,role):
        event_type = 6
        print ("Storing Assume Role Event with role %s ..." % role)
        fb = self.__registerSubmitterDeviceOnDB(submitter_id)
        if fb[0] != 0:
            return fb

        fb = self.__DBM.storeTableEntry('events', [submitter_id, ts, event_type])
        if fb[0] != 0:
            return fb[0], 'Error while storing Assume Role Event:  ' + fb[1]

        res = self.__DBM.getTuple('events', submitter_id=submitter_id, timestamp=ts, type=event_type)

        if res[0] > 0:
            return res[0], 'Error while storing Assume Role Event: ' + res[1]

        if (res[0] == 0 and (res[1] is None)):
            raise Exception('entry not present on DB while previously stored, EXIT')

        event_id = res[1][0]
        fb = self.__DBM.storeTableEntry('typeEvent_assume_role',
                                        [event_id,role])

        if fb[0] != 0:
            print('ERROR: ', fb[1])
            raise Exception('Impossible to store entry in typeEvent_assume_role table, DB integrity compromised, EXIT')

        return 0, None

    def storeScanEvent(self,ts,submitter_id,status):
        event_type = 7
        print ("Storing Scan Event with submitter-id - %s - timestamp %s -  status %s ..." % (submitter_id,ts,status))
        fb = self.__registerSubmitterDeviceOnDB(submitter_id)
        if fb[0] != 0:
            return fb

        fb = self.__DBM.storeTableEntry('events', [submitter_id, ts, event_type])
        if fb[0] != 0:
            return fb[0], 'Error while storing Scan Event:  ' + fb[1]

        res = self.__DBM.getTuple('events', submitter_id=submitter_id, timestamp=ts, type=event_type)

        if res[0] > 0:
            return res[0], 'Error while storing Scan Event: ' + res[1]

        if (res[0] == 0 and (res[1] is None)):
            raise Exception('entry not present on DB while previously stored, EXIT')

        event_id = res[1][0]
        fb = self.__DBM.storeTableEntry('typeEvent_scan',
                                        [event_id,status])

        if fb[0] != 0:
            print('ERROR: ', fb[1])
            raise Exception('Impossible to store entry in typeEvent_scan table, DB integrity compromised, EXIT')

        return 0, None


    def __registerSubmitterDeviceOnDB(self, submitter_id):
        fb = self.__DBM.checkTupleExists('devices', submitter_id)

        if fb[0] != 0:
            return fb[0], 'Error while checking the existence of a device:  ' + fb[1]

        if fb[0] == 0 and fb[1] == 0:
            self.__DBM.storeTableEntry('devices', [submitter_id])

        return 0, None

    def close(self):
        self.__DBM.closeConn()