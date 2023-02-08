import os
import logging as log
import pandas as pd

from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

def verify_data_mysql(conn_id:str, query:str, columns:list, data:list[tuple]):
    """
    Function verify_data_mysql
    param conn_id: MySQL connection id stored in Airflow Connection
    param query: query sql for selecting data from target table in MySQL
    param columns: column mapping info of target table in MySQL
    param data: test data to be compared with the data in MySQL

    return True when verify data result passed
    """
    hook = BaseHook.get_hook(conn_id)
    # fetch data from MySQL
    df_msql = pd.DataFrame()
    if isinstance(hook, MySqlHook):
        df_msql = hook.get_pandas_df(query)
    # fetch data from test data
    df = pd.DataFrame(data, columns = columns)
    # verify data result
    result = df.reset_index(drop=True).equals(df_msql.reset_index(drop=True))  
    if not result:
        raise AirflowException('Verify data result failed!')
    else:
        log.info('Verify data result passed!')
    
    return True


def is_file_deleted(file_path:str, **context):
    """
    Function is_file_deleted
    param file_path: file path to be checked 
    param **context: airflow DAG running context

    return True when target file has been deleted
    """
    if(os.path.isfile(file_path) and os.stat(file_path).st_size > 0):
        log.info('Target file was not deleted!')
        raise AirflowException(
            f'Target file "{file_path}" was not deleted!'
        )
    else:
        log.info('Target file deleted!')

    return True