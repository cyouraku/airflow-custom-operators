"""
Test Transfer data from Oracle to MySQL using the custom operator 'DataTransferToMysql'

Oracle version: 19.3.0
MySQL version: 5.7.40-debian

Author: Tim Zhang (tim.zhang1981@gmail.com)

"""
import pendulum

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.operators.oracle import OracleStoredProcedureOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from common.operators.data_transfer_to_mysql import DataTransferToMysql
from common.utils.utils import is_file_deleted, verify_data_mysql

TMP_PATH = '/opt/airflow/tmp'
CSV_FILE_PATH = f'{TMP_PATH}/test_oracle2mysql.sql'
TEST_DATA_ROWS = 8000 # create 8000 rows of test data

# prepare test data
def getTestData(rows:int):
    prefix = 'test-2234-3234'
    data = []
    for i in list(range(rows)):
        col1 = i + 1
        padding = '{:>04d}'.format(col1)
        dummy_data = f'{prefix}-{padding}'
        row = (col1, dummy_data, dummy_data, dummy_data, dummy_data, dummy_data, dummy_data, dummy_data, dummy_data)
        data.append(row)
    return data

# prepare test data for Oracle
def prepareTestData4Oracle(conn_id:str, data:list[tuple], **context):
    hook = BaseHook.get_hook(conn_id)
    if isinstance(hook, OracleHook):
        hook.bulk_insert_rows('test_oracle_to_mysql', data)

with DAG(
    dag_id="test_oracle2mysql",
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test", "airflow-custom-operators"],
) as dag:

        ORACLE_CONN_ID = 'oracle_sandbox'
        MYSQL_CONN_ID = 'mysql_sandbox'

        start = EmptyOperator(
            task_id="start",
        )

        end = EmptyOperator(
            task_id="end",
        )
      
        # create test table
        create_oracle_table_bef_test = OracleStoredProcedureOperator(
            task_id = 'create_oracle_table_bef_test',
            oracle_conn_id = ORACLE_CONN_ID,
            procedure = './sql/test_oracle2mysql/oracle_proc_create_table_bef_test.sql'
        )

        # insert test data into Oracle before test
        test_data = getTestData(TEST_DATA_ROWS)
        prepare_data_bef_test = PythonOperator(
            task_id = 'prepare_data_bef_test',
            python_callable = prepareTestData4Oracle,
            op_args = [ORACLE_CONN_ID, test_data],
            provide_context = True
        )

        # test custom operator 'DataTransferToMysql'
        # transfer data from Oracle to MySQL
        transfer_data_to_mysql = DataTransferToMysql(
            task_id = 'transfer_data_to_mysql',
            csv_file = CSV_FILE_PATH,
            max_size = 1 * 1024 * 1024, # set max chunk size as 1024KB to test split file by chunks while uploading
            sql = './sql/test_oracle2mysql/',
            source_conn_id = ORACLE_CONN_ID,
            destination_conn_id = MYSQL_CONN_ID,
            destination_table = 'test_oracle_to_mysql',
            preoperator = './sql/test_oracle2mysql/mysql_preoperator.sql' # prepare test table in MySQL before data transfer
        )

        # verify test data tansfered into MySQL correctly or not after test
        v_query = 'select col1, col2, col3, col4, col5, col6, col7, col8, col9 from test_oracle_to_mysql;'
        v_columns = ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9']
        verify_upload_data_result = PythonOperator(
            task_id = 'verify_upload_data_result',
            python_callable = verify_data_mysql,
            op_args = [MYSQL_CONN_ID, v_query, v_columns, test_data],
            provide_context = True
        )

        # check tmp csv file deleted after test
        check_file_deleted = PythonOperator(
            task_id = 'check_file_deleted',
            python_callable = is_file_deleted,
            op_args = [CSV_FILE_PATH],
            provide_context = True
        )

        # drop test table in Oracle after test
        clear_oracle_aft_test = OracleStoredProcedureOperator(
            task_id = 'clear_oracle_aft_test',
            oracle_conn_id = ORACLE_CONN_ID,
            procedure = './sql/test_oracle2mysql/oracle_proc_drop_table_aft_test.sql'
        )

        start >> create_oracle_table_bef_test \
        >> prepare_data_bef_test >> transfer_data_to_mysql \
        >> verify_upload_data_result >> check_file_deleted \
        >> clear_oracle_aft_test>> end