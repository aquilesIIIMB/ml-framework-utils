from interface import implements
import pandas
import pyodbc
from connections.config.mssql_config import generate_connection_string
from connections.repository_interface import RepositoryInterface

class MSSQLServer(implements(RepositoryInterface)):
    def __init__(self, config):
        connection_string = generate_connection_string(config)
        print('Connecting to PDW <br>')
        self.connection = pyodbc.connect(connection_string, timeout=60)
        print('Successfully connected to PDW <br>')
        self.cursor = self.connection.cursor()
        self.allow_subsequent_transactions()

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_val, exc_tb):
        self.close()

    def allow_subsequent_transactions(self):
        try:
            self.cursor.execute('create table #connection_workaround (id int)')
        except Exception as exception:
            print(f'Ignoring expected initial operation transaction error: {str(exception)}')

    def write(self, dataframe, target_name):
        raise NotImplementedError(f'Attempting to load DataFrame {dataframe} into table {target_name}')

    def execute(self, query, args=None):
        if args is None:
            return self.cursor.execute(query)
        return self.cursor.execute(query, args)

    def execute_values(self, query, args):
        if args:
            self.cursor.executemany(query, args)

    def read(self, query_template, args=None):
        if args is None:
            self.cursor.execute(query_template)
        else:
            self.cursor.execute(query_template, args)
        data = self.cursor.fetchall()
        columns = [column[0] for column in self.cursor.description]
        dataframe = pandas.DataFrame((tuple(t) for t in data), columns=columns)
        return dataframe

    def log_table(self, table_name):
        data_cursor = self.execute(f'SELECT * FROM {table_name} ORDER BY 1 ASC')
        dataframe = pandas.DataFrame([list(row) for row in data_cursor])
        if any(s not in table_name for s in '#'):
            column_names = [self.cursor.description[i][0] for i in range(len(self.cursor.description))]
            dataframe.columns = column_names
        print(f'Table {table_name} contents: ')
        print(dataframe.to_string())

    def commit(self):
        return self

    def close(self):
        self.connection.close()
        return self
