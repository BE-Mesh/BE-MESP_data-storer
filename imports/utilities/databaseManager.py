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
                message  = 'SQL Integrity error ' + str(e)
                return 11,message

        elif(table_name == 'events'):
            if len(entries) < 3:
                return 10, 'Error while storing events entry in Db, not enough arguments'
            try:
                cur.execute ("INSERT INTO {tn} ({c1}, {c2}, {c3}) VALUES ('{sub_id}', '{ts}', '{type}')". \
                           format (tn=table_name, c1='submitter_id', c2='timestamp', c3='type',
                                   sub_id=entries[0], ts=entries[1],type=entries[2]))
            except sqlite3.IntegrityError as e:
                self.__conn.close()
                message = 'SQL Integrity error ' + str(e)
                return 11, message

        elif (table_name == 'typeEvent_message_sent'):
            if len(entries) < 6:
                return 10, 'Error while storing typeEvent_message_sent entry in Db, not enough arguments'
            try:
                cur.execute("INSERT INTO {tn} ({c1}, {c2}, {c3}, {c4}, {c5}, {c6}) "
                            "VALUES ('{ev_id}', '{s_id}', '{rec_id}', "
                            "'{nx_hop_id}', '{m_type}', '{payld}')". \
                            format(tn=table_name, c1='event_id', c2='sender_id', c3='receiver_id',
                                   c4='next_hop_id', c5='message_type', c6='payload',
                                   ev_id=entries[0], s_id=entries[1], rec_id=entries[2],
                                   nx_hop_id=entries[3], m_type= entries[4],payld = entries[5]))
            except sqlite3.IntegrityError as e:
                self.__conn.close()
                message = 'SQL Integrity error ' + str(e)
                return 11, message

        elif (table_name == 'typeEvent_message_received'):
            if len(entries) < 6:
                return 10, 'Error while storing typeEvent_message_received entry in Db, not enough arguments'
            try:
                cur.execute("INSERT INTO {tn} ({c1}, {c2}, {c3}, {c4}, {c5}, {c6}) "
                            "VALUES ('{ev_id}', '{s_id}', '{rec_id}', "
                            "'{prev_hop_id}', '{m_type}', '{payld}')". \
                            format(tn=table_name, c1='event_id', c2='sender_id', c3='receiver_id',
                                   c4='prev_hop_id', c5='message_type', c6='payload',
                                   ev_id=entries[0], s_id=entries[1], rec_id=entries[2],
                                   prev_hop_id=entries[3], m_type=entries[4], payld=entries[5]))
            except sqlite3.IntegrityError as e:
                self.__conn.close()
                message = 'SQL Integrity error ' + str(e)
                return 11, message

        elif (table_name == 'typeEvent_outgoing_connection_attempts'):
            if len(entries) < 2:
                return 10, 'Error while storing typeEvent_outgoing_connection_attempts entry in Db, not enough arguments'
            try:
                cur.execute("INSERT INTO {tn} ({c1}, {c2}) "
                            "VALUES ('{ev_id}', '{target_id}')". \
                            format(tn=table_name, c1='event_id', c2='target_id',
                                   ev_id=entries[0], target_id=entries[1]))
            except sqlite3.IntegrityError as e:
                self.__conn.close()
                message = 'SQL Integrity error ' + str(e)
                return 11, message

        elif (table_name == 'typeEvent_incoming_connection_attempts'):
            if len(entries) < 2:
                return 10, 'Error while storing typeEvent_incoming_connection_attempts entry in Db, not enough arguments'
            try:
                cur.execute("INSERT INTO {tn} ({c1}, {c2}) "
                            "VALUES ('{ev_id}', '{requester_id}')". \
                            format(tn=table_name, c1='event_id', c2='requester_id',
                                   ev_id=entries[0], requester_id=entries[1]))
            except sqlite3.IntegrityError as e:
                self.__conn.close()
                message = 'SQL Integrity error ' + str(e)
                return 11, message

        elif (table_name == 'typeEvent_connection_attempts_outcomes'):
            if len(entries) < 4:
                return 10, 'Error while storing typeEvent_connection_attempts_outcomes entry in Db, not enough arguments'
            try:
                cur.execute("INSERT INTO {tn} ({c1}, {c2}, {c3}, {c4}) "
                            "VALUES ('{ev_id}', '{req_id}', '{target_id}', "
                            "'{outcome}')". \
                            format(tn=table_name, c1='event_id', c2='requester_id', c3='target_id',
                                   c4='outcome',
                                   ev_id=entries[0], req_id=entries[1], target_id=entries[2],
                                   outcome=entries[3]))
            except sqlite3.IntegrityError as e:
                self.__conn.close()
                message = 'SQL Integrity error ' + str(e)
                return 11, message

        else:
            return 12,'No DB table found'

        self.__conn.commit()
        return 0,None


    #todo: sviluppare le altre tabelle
    def checkTupleExists(self, table_name, pk, opt_fist=None,opt_second=None, opt_third=None):
        self.__conn = sqlite3.connect(self.__sql_database_path)
        cur = self.__conn.cursor()
        if (table_name == 'devices'):
            # if len(entries) < 1:
            #     return 10, 'Error while storing devices entry in Db, not enough arguments'

            try:

                cur.execute("SELECT COUNT(*) FROM {tn} WHERE {c1} = '{v1}'". \
                            format(tn=table_name, c1='BLE_address', v1=pk))

                record = cur.fetchall()[0][0]

            except sqlite3.IntegrityError as e:
                self.__conn.close()
                message = 'SQL Integrity error ' + str(e)
                return 11, message

            try:
                record= int(record)
            except ValueError:
                return 12,'Wrong value returned by SQL query in devices,it is not a number'

            return 0,record

        else:
            return 12,'No table found'


    #TODO
    def getTuple(self, table_name, BLE_address='NULL', timestamp='NULL', event_id='NULL', submitter_id='NULL', type= 'NULL',
                 id='NULL', role='NULL', requester_id='NULL', target_id='NULL', outcome='NULL', sender_id='NULL',
                 receiver_id='NULL', prev_hop_id='NULL', message_type='NULL', payload= 'NULL', next_hop_id='NULL',
                 start_or_stop='NULL'):
        self.__conn = sqlite3.connect(self.__sql_database_path)
        cur = self.__conn.cursor()

        if BLE_address != 'NULL':
            BLE_address = '"'+ BLE_address + '"'
        if timestamp != 'NULL':
            timestamp = '"'+ timestamp + '"'
        if event_id != 'NULL':
            event_id = '"'+ event_id + '"'
        if submitter_id != 'NULL':
            submitter_id = '"'+ submitter_id + '"'
        if type != 'NULL':
            type = '"'+ str(type) + '"'
        if id != 'NULL':
            id = '"'+ id + '"'
        if role != 'NULL':
            role = '"'+ role + '"'
        if requester_id != 'NULL':
            requester_id = '"'+ requester_id + '"'
        if target_id != 'NULL':
            target_id = '"'+ target_id + '"'
        if outcome != 'NULL':
            outcome = '"'+ outcome + '"'
        if sender_id != 'NULL':
            sender_id = '"'+ sender_id + '"'
        if receiver_id != 'NULL':
            receiver_id = '"'+ receiver_id + '"'
        if prev_hop_id != 'NULL':
            prev_hop_id = '"'+ prev_hop_id + '"'
        if message_type != 'NULL':
            message_type = '"'+ message_type + '"'
        if payload != 'NULL':
            payload = '"'+ payload + '"'
        if next_hop_id != 'NULL':
            next_hop_id = '"' + next_hop_id + '"'
        if start_or_stop != 'NULL':
            start_or_stop = '"'+ start_or_stop + '"'

        if (table_name == 'devices'):
            if BLE_address == 'NULL':
                return 1,'not enough params passed in getTuple for table devices'

            try:
                cur.execute("SELECT * FROM {tn} WHERE (({v1} IS NULL OR {c1}={v1}))". \
                            format(tn=table_name, c1='BLE_address', v1=BLE_address, c2='timestamp', v2=timestamp,
                                   c3='event_id', v3=event_id, c4='submitter_id', v4=submitter_id, c5='type', v5=type,
                                   c6='id', v6=id, c7='role', v7=role, c8='requester_id', v8=requester_id,
                                   c9='target_id', v9=target_id, c10='outcome', v10=outcome, c11='sender_id',
                                   v11=sender_id,
                                   c12='receiver_id', v12=receiver_id, c13='prev_hop_id', v13=prev_hop_id,
                                   c14='message_type', v14=message_type, c15='payload', v15=payload,
                                   c16='next_hop_id', v16=next_hop_id, c17='start_or_stop', v17=start_or_stop,
                                   ))

                record = cur.fetchone()

            except sqlite3.IntegrityError as e:
                self.__conn.close()
                message = 'SQL Integrity error ' + str(e)
                return 11, message


        #todo: fai l if per ogni tabella
        elif (table_name == 'events'):
            if submitter_id == 'NULL':
                return 1,'not enough params passed in getTuple for table devices'

            try:
                cur.execute("SELECT * FROM {tn} WHERE (({v3} IS NULL OR {c3}={v3}) AND ({v4} IS NULL OR {c4}={v4})"
                            "AND ({v2} IS NULL OR {c2}={v2}) AND ({v5} IS NULL OR {c5}={v5}) )". \
                            format(tn=table_name, c1='BLE_address', v1=BLE_address, c2='timestamp', v2=timestamp,
                                   c3='event_id', v3=event_id, c4='submitter_id', v4=submitter_id, c5='type', v5=type,
                                   c6='id', v6=id, c7='role', v7=role, c8='requester_id', v8=requester_id,
                                   c9='target_id', v9=target_id, c10='outcome', v10=outcome, c11='sender_id',
                                   v11=sender_id,
                                   c12='receiver_id', v12=receiver_id, c13='prev_hop_id', v13=prev_hop_id,
                                   c14='message_type', v14=message_type, c15='payload', v15=payload,
                                   c16='next_hop_id', v16=next_hop_id, c17='start_or_stop', v17=start_or_stop,
                                   ))

                record = cur.fetchone()

            except sqlite3.IntegrityError as e:
                self.__conn.close()
                message = 'SQL Integrity error ' + str(e)
                return 11, message


        elif (table_name == 'typeEvent_message_sent'):
            if sender_id == 'NULL':
                return 1,'not enough params passed in getTuple for table devices'

            try:
                cur.execute("SELECT * FROM {tn} WHERE (({v6} IS NULL OR {c6}={v6}) AND ({v3} IS NULL OR {c3}={v3})"
                            "AND ({v11} IS NULL OR {c11}={v11}) AND ({v12} IS NULL OR {c12}={v12}) "
                            "AND ({v16} IS NULL OR {c16}={v16}) AND ({v14} IS NULL OR {c14}={v14})"
                            "AND ({v14} IS NULL OR {c14}={v14}) )". \
                            format(tn=table_name, c1='BLE_address', v1=BLE_address, c2='timestamp', v2=timestamp,
                                   c3='event_id', v3=event_id, c4='submitter_id', v4=submitter_id, c5='type', v5=type,
                                   c6='id', v6=id, c7='role', v7=role, c8='requester_id', v8=requester_id,
                                   c9='target_id', v9=target_id, c10='outcome', v10=outcome, c11='sender_id',
                                   v11=sender_id,
                                   c12='receiver_id', v12=receiver_id, c13='prev_hop_id', v13=prev_hop_id,
                                   c14='message_type', v14=message_type, c15='payload', v15=payload,
                                   c16='next_hop_id', v16=next_hop_id, c17='start_or_stop', v17=start_or_stop,
                                   ))

                record = cur.fetchone()

            except sqlite3.IntegrityError as e:
                self.__conn.close()
                message = 'SQL Integrity error ' + str(e)
                return 11, message


        else:
            return 12, 'No table found'

        #
        # try:
        #     cur.execute("SELECT * FROM {tn} WHERE (({v1} IS NULL OR {c1}={v1}) AND ({v2} IS NULL OR {c2}={v2})"
        #                 "AND ({v3} IS NULL OR {c3}={v3}) AND ({v4} IS NULL OR {c4}={v4}) "
        #                 "AND ({v5} IS NULL OR {c5}={v5}) AND ({v6} IS NULL OR {c6}={v6})"
        #                 "AND ({v7} IS NULL OR {c7}={v7}) AND ({v8} IS NULL OR {c8}={v8})"
        #                 "AND ({v9} IS NULL OR {c9}={v9}) AND ({v10} IS NULL OR {c10}={v10})"
        #                 "AND ({v11} IS NULL OR {c11}={v11}) AND ({v12} IS NULL OR {c12}={v12})"
        #                 "AND ({v13} IS NULL OR {c13}={v13}) AND ({v14} IS NULL OR {c14}={v14})"
        #                 "AND ({v15} IS NULL OR {c15}={v15}) AND ({v16} IS NULL OR {c16}={v16})"
        #                 "AND ({v17} IS NULL OR {c17}={v17}) )". \
        #                 format(tn=table_name, c1='BLE_address', v1=BLE_address, c2='timestamp', v2=timestamp,
        #                        c3='event_id', v3=event_id, c4='submitter_id', v4=submitter_id, c5='type', v5=type,
        #                        c6='id', v6=id, c7='role', v7=role, c8='requester_id', v8=requester_id,
        #                        c9='target_id', v9=target_id, c10='outcome', v10=outcome, c11='sender_id', v11=sender_id,
        #                        c12='receiver_id', v12=receiver_id, c13='prev_hop_id', v13=prev_hop_id,
        #                        c14='message_type', v14=message_type, c15='payload', v15=payload,
        #                        c16='next_hop_id', v16=next_hop_id, c17='start_or_stop', v17=start_or_stop,
        #                        ))
        #
        #     record = cur.fetchone()
        #
        # except sqlite3.IntegrityError as e:
        #     self.__conn.close()
        #     message = 'SQL Integrity error ' + str(e)
        #     return 11, message

        return 0, record

    #TODO: sviluppare meccanismo check se la connessione è chiusa
    def closeConn(self):
        self.__conn.close()













