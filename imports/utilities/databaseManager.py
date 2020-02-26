import sqlite3


class DatabaseManager():

    def __init__(self,sql_database_path):
        self.__sql_database_path = sql_database_path
        self.__conn = sqlite3.connect(self.__sql_database_path)





    def createTable(self, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            cur = self.__conn.cursor()
            cur.execute(create_table_sql)
        except Exception as e:
            print('E: error occurred while executing the following sql command: ',create_table_sql)
            raise e
        pass

    #TODO: finire la struttura degli insert per le varie tabelle
    def storeTableEntry(self,table_name,entries):
        self.__conn = sqlite3.connect(self.__sql_database_path)
        cur = self.__conn.cursor()

        if (table_name == 'devices'):
            if len(entries) < 1:
                return 10, 'Error while storing devices entry in Db, not enough arguments'

            try:
                cur.execute ("INSERT INTO {tn}({c1}) VALUES ('{address}')". \
                           format (tn=table_name, c1='BLE_address', address=entries[0]))
            except sqlite3.IntegrityError as e:
                self.__conn.close()
                return 11,'SQL Integlity error: ' + e

        elif(table_name == 'events'):
            if len(entries) < 3:
                return 10, 'Error while storing events entry in Db, not enough arguments'
            try:
                cur.execute ("INSERT INTO {tn} ({c1}, {c2}, {c3}) VALUES ('{sub_id}', {ts}, {type})". \
                           format (tn=table_name, c1='submitter_id', c2='timestamp', c3='type',
                                   sub_id=entries[0], ts=entries[1],type=entries[2]))
            except sqlite3.IntegrityError as e:
                self.__conn.close()
                return 11, 'SQL Integlity error: ' + e

        else:
            return 12,'No DB table found'

        self.__conn.commit()
        return 0,None


    def closeConn(self):
        self.__conn.close()













