import os
import sqlite3 as lite


class DatabaseManager:

    def __init__(self):
        self.sql_connection: lite.Connection = None

    def connect(self):
        db_file_name = "vereinsdaten.sqlite"
        if os.path.isfile(str(os.path.join(os.getcwd(), db_file_name))):
            try:
                self.sql_connection = lite.connect(db_file_name)
                curs = self.sql_connection.cursor()
                curs.execute("SELECT * FROM mitglieder LIMIT 1")
                curs.execute("PRAGMA foreign_keys = ON;")
            except lite.OperationalError:
                self.sql_connection = None
                print("Failed to establish connection")
        else:
            print(f"DB-CONNECTOR: Datebase file '{db_file_name}' does not exist in {os.getcwd()}!")

    def disconnect(self):
        if self.is_connected():
            self.sql_connection.close()
            self.sql_connection = None
            print(f"DB-CONNECTOR: Connection successfully closed.")
        else:
            if self.sql_connection is None:
                print("DB-CONNECTOR: Connection was already closed.")
            else:
                self.sql_connection = None

    def is_connected(self) -> bool:
        """
        runs at select query to confirm a valid connection
        :return: True if query was successful, False if not
        """
        try:
            self.sql_connection.execute("SELECT * FROM mitglieder LIMIT 1")
            return True
        except (AttributeError, lite.OperationalError) as e:
            self.sql_connection = None
            return False

    def execute_query(self, query_str: str) -> list:
        try:
            cursor = self.sql_connection.cursor()
            cursor.execute(query_str)
            result = cursor.fetchall()
            cursor.close()
            return result
        except (AttributeError, lite.OperationalError) as e:
            if type(e).__name__ == 'AttributeError':
                print(f"DB-CONNECTOR: Query: '{query_str}' could not be executed - REASON: No connection")
                self.sql_connection = None
            elif type(e).__name__ == 'OperationalError':
                print(f"DB-CONNECTOR: Query: '{query_str}' could not be executed - REASON: Invalid command syntax")

    def execute_insert(self, tablename: str, **kwargs) -> int:
        try:
            cursor = self.sql_connection.cursor()
            value_names = ""
            questionmark_string = ""
            values = []
            first = True
            for k in kwargs.keys():
                if first:
                    first = False
                else:
                    questionmark_string += ","
                    value_names += ","
                value_names += k
                values.append(kwargs.get(k))
                questionmark_string += "?"

            ex_string = f"INSERT INTO {tablename} ({value_names}) values ({questionmark_string});"

            cursor.execute(ex_string, values)
            self.sql_connection.commit()
            retVal = cursor.lastrowid
            cursor.close()
            return retVal
        except(AttributeError, lite.OperationalError, lite.IntegrityError) as e:
            if type(e).__name__ == 'AttributeError':
                print(f"DB-CONNECTOR: Insert Operation into '{tablename}' with values '{kwargs.values()}' could not "
                      f"be executed - REASON: No connection")
                self.sql_connection = None
            elif type(e).__name__ == 'OperationalError':
                print(f"DB-CONNECTOR: Insert Operation into '{tablename}' with values '{kwargs.values()}' could not "
                      f"be executed - REASON: Invalid command syntax")
            elif type(e).__name__ == 'IntegrityError':
                print(f"DB-CONNECTOR: Insert Operation into '{tablename}' with values '{values}' could not "
                      f"be executed - REASON: {e.args[0]}")
            return None
