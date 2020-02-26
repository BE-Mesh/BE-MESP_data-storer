from .utilities.singleton import Singleton
from .utilities.databaseManager import DatabaseManager
from pathlib import Path
import re
import sys
import os
import datetime


class Storage(metaclass=Singleton):
    def __init__(self):

        self.__database_path = self.__createDB()[1]
        self.__DBM = DatabaseManager(self.__database_path)
        self.__initializeDBstructure()




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
                                        next_hop_id text,
                                        message_type text NOT NULL,
                                        payload text NOT NULL,
                                        FOREIGN KEY (event_id) REFERENCES events (event_id)
                                    );"""

        sql_create_table_TE_message_received = """CREATE TABLE IF NOT EXISTS typeEvent_message_received (
                                        id integer PRIMARY KEY AUTOINCREMENT,
                                        event_id text NOT NULL UNIQUE,
                                        sender_id text NOT NULL,
                                        receiver_id text NOT NULL,
                                        prev_hop_id text,
                                        message_type text NOT NULL,
                                        payload text NOT NULL,
                                        FOREIGN KEY (event_id) REFERENCES events (event_id)
                                    );"""

        sql_create_table_TE_incoming_connection_attempts = """CREATE TABLE IF NOT EXISTS typeEvent_incoming_connection_attempts (
                                                id integer PRIMARY KEY AUTOINCREMENT,
                                                event_id text NOT NULL UNIQUE,
                                                requester_id text NOT NULL,
                                                notes text,
                                                FOREIGN KEY (event_id) REFERENCES events (event_id)
                                            );"""

        sql_create_table_TE_outgoing_connection_attempts = """CREATE TABLE IF NOT EXISTS typeEvent_outgoing_connection_attempts (
                                                id integer PRIMARY KEY AUTOINCREMENT,
                                                event_id text NOT NULL UNIQUE,
                                                target_id text NOT NULL,
                                                notes text,
                                                FOREIGN KEY (event_id) REFERENCES events (event_id)
                                            );"""

        sql_create_table_TE_connection_attempts_outcomes = """CREATE TABLE IF NOT EXISTS typeEvent_connection_attempts_outcomes (
                                                id integer PRIMARY KEY AUTOINCREMENT,
                                                event_id text NOT NULL UNIQUE,
                                                requester_id text NOT NULL,
                                                target_id text NOT NULL,
                                                outcome text NOT NULL,
                                                notes text,
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
                                                        start_or_stop text NOT NULL,
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
    #TODO: settare il check che il campo message tyope su message received or send sia un intero
    def storeMessageSentEvent(self,ts,submitter_id,sender,receiver,next_hop,message_type,payload):
        event_type = 0
        print ("Storing Message-Sent Event...")

        self.__DBM.storeTableEntry('devices',[submitter_id])
        self.__DBM.storeTableEntry('events', [submitter_id,ts,event_type])

        return 0,None

    def storeMessageRcvEvent(self,ts,submitter_id,sender,receiver,prev_hop,message_type,payload):
        event_type = 1
        print ("Storing Message-Received Event...")
        return 0,None

    def storeOutgoingConnectionAttemptEvent(self,ts,submitter_id,connect_to):
        event_type = 2
        print ("Storing Outgoing ConnectionAttempt Event...")
        return 0,None

    def storeIncomingConnectionAttemptEvent(self,ts,submitter_id,connect_from):
        event_type = 3
        print ("Storing Incoming ConnectionAttempt Event...")
        return 0,None

    def storeConnectionAttemptResultEvent(self,ts,submitter_id,connect_from,connect_to,outcome):
        event_type = 4
        print ("Storing ConnectionAttemptResult Event with result %s..." % outcome)
        return 0,None

    def storeDeviceUpEvent(self,ts,submitter_id):
        event_type = 5
        print ("Storing Device Up Event...")
        return 0,None

    def storeAssumeRoleEvent(self,ts,submitter_id,role):
        event_type = 6
        print ("Storing Assume Role Event with role %s ..." % role)
        return 0,None

    def storeScanEvent(self,ts,submitter_id,status):
        event_type = 7
        print ("Storing Scan Event with status %s ..." % status)
        return 0,None

    def close(self):
        self.__DBM.closeConn()