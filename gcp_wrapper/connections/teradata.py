from mlutils import connector
from interface import implements
import pandas as pd
from connections.repository_interface import RepositoryInterface

class Teradata(implements(RepositoryInterface)):
    def __init__(self, config):
        self.connection = connector.get_connector(name=config)
        print('Successfully connected to Teradata')

    def __enter__(self):
        pass

    def __exit__(self, ext_type, exc_val, exc_tb):
        self.close()

    def write(self, dataframe, target_name):
        raise NotImplementedError(f'Attempting to load DataFrame {dataframe} into table {target_name}')

    # pylint: disable=W0613
    def read(self, query_template, args=None):
        resoverall = self.connection.execute(query_template)
        dataframe = pd.DataFrame(resoverall.fetchall())
        dataframe.columns = resoverall.keys()
        return dataframe

    def commit(self):
        pass

    def close(self):
        pass
