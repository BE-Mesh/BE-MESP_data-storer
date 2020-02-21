import sqlite3


class DatabaseManager():

    def __init__(self,sql_database_path):
        self.__sql_database_path = sql_database_path
        self.__conn = sqlite3.connect(self.__sql_database_path)





    def create_table(self, create_table_sql):
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



