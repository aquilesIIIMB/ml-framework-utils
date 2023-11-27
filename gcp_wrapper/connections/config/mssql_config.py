import os
import platform
from munch import Munch
from mlutils.nbhelper import getArgument
from connections.config.yaml_formatter import read_formated_yaml_file

def get_pdw_element_workflow_global_parameters():
    mssql_config = {
        'host': '{}'.format(getArgument('PDW_HOST','PARAMS')),
        'database': '{}'.format(getArgument('PDW_NAME','PARAMS')),
        'user': '{}'.format(getArgument('PDW_USERNAME','PARAMS')),
        'password': '{}'.format(getArgument('PDW_PASSWORD','PARAMS'))
    }
    if mssql_config['host'] == 'None':
        raise Exception('No global parameters detected')
    return mssql_config

def get_pdw_environment_variables():
    mssql_config = {
        'host': '{}'.format(os.environ['PDW_HOST']),
        'database': '{}'.format(os.environ['PDW_NAME']),
        'user': '{}'.format(os.environ['PDW_USERNAME']),
        'password': '{}'.format(os.environ['PDW_PASSWORD']),
    }
    return mssql_config

def get_pdw_from_yaml_file():
    mssql_config = read_formated_yaml_file("pdw_con.yaml")
    if mssql_config['host'] == 'None':
        raise Exception('No global parameters detected')
    return mssql_config

def get_mssql_config():
    try:
        mssql_config = get_pdw_element_workflow_global_parameters()
    except:
        try:
            mssql_config = get_pdw_from_yaml_file()
        except:
            try:
                mssql_config = get_pdw_environment_variables()
            except:
                print('No luck buddy')
    return mssql_config

def generate_connection_string(config):
    if config != 'None':
        driver = 'ODBC Driver 17 for SQL Server'
        mssql_config = Munch(config)
    else:
        config_dict = get_mssql_config()
        mssql_config = Munch(config_dict)
        if platform.system() == 'Linux':
            driver = 'ODBC Driver 13 for SQL Server'
        else:
            driver = 'ODBC Driver 17 for SQL Server'
    connection_string = f'DRIVER={driver};SERVER={mssql_config.host};DATABASE={mssql_config.database};UID={mssql_config.user};PWD={mssql_config.password}'
    return connection_string
