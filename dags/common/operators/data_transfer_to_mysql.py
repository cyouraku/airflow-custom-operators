from __future__ import annotations

from typing import Sequence
from airflow.hooks.base import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.context import Context
import os
import pandas as pd


class DataTransferToMysql(BaseOperator):
    """
    Based on Airflow's GenericTransfer operator

    Improved the uploading data to mysql with the 'load data local infile' method instead.

    Author: Tim Zhang (tim.zhang1981@gmail.com)

    :param sql: SQL query to execute against the source database. (templated)
    :param destination_table: target table. (templated)
    :param source_conn_id: source connection
    :param destination_conn_id: destination connection
    :param csv_file: csv file path used to save data to csv file and upload data to MySQL
    :param max_size: for large data, the csv file would be uploaded to MySQL by loop according to the max_size configured
    :param duplicate_key_handling: MySQL feature for loading data by "LOAD DATA LOCAL INFILE"
    :param extra_options: MySQL feature for loading data by "LOAD DATA LOCAL INFILE"
    :param preoperator: sql statement or list of statements to be
        executed prior to loading the data. (templated)
    :param insert_args: extra params for `insert_rows` method.
    """

    template_fields: Sequence[str] = ("sql", "destination_table", "preoperator")
    template_ext: Sequence[str] = (
        ".sql",
        ".hql",
    )
    template_fields_renderers = {"preoperator": "sql"}
    ui_color = "#b0f07c"

    def __init__(
        self,
        *,
        sql: str,
        destination_table: str,
        source_conn_id: str,
        destination_conn_id: str,
        csv_file: str,
        max_size = 50 * 1024 * 1024,
        duplicate_key_handling = 'IGNORE',
        extra_options = '''
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        ''',
        preoperator: str | list[str] | None = None,
        insert_args: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.destination_table = destination_table
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.csv_file = csv_file
        self.max_size = max_size
        self.duplicate_key_handling = duplicate_key_handling
        self.extra_options = extra_options
        self.preoperator = preoperator
        self.insert_args = insert_args or {}

    def execute(self, context: Context):
        source_hook = BaseHook.get_hook(self.source_conn_id)      
        self.log.info("Extracting data from %s", self.source_conn_id)
        self.log.info("Executing: \n %s", self.sql)
        get_records = getattr(source_hook, "get_records", None)

        if not callable(get_records):
            raise RuntimeError(
                f"Hook for connection {self.source_conn_id!r} "
                f"({type(source_hook).__name__}) has no `get_records` method"
            )
        else:
            results = get_records(self.sql)
            # use pandas dataframe to do the ETL processing 
            df = pd.DataFrame(results)
            # filter N/A value to '\\n' for MySQL to load data from CSV file
            df = df.fillna('\\N')
            # save data to CSV file
            df.to_csv(self.csv_file, index = False, header = False, quotechar = "'")

        destination_hook = BaseHook.get_hook(self.destination_conn_id)
        if self.preoperator:
            run = getattr(destination_hook, "run", None)
            if not callable(run):
                raise RuntimeError(
                    f"Hook for connection {self.destination_conn_id!r} "
                    f"({type(destination_hook).__name__}) has no `run` method"
                )
            self.log.info("Running preoperator")
            self.log.info(self.preoperator)
            run(self.preoperator)

        self.log.info("Uploading data into %s", self.destination_conn_id)
        self.uploadByChunk(destination_hook)
        # delete CSV file after uploading
        os.remove(self.csv_file)

    def uploadByChunk(self, hook):
        part_file = f'{self.csv_file}.part'
        part_id = 0

        with open(self.csv_file) as fin:
            fout = open(part_file, "w")
            for _, line in enumerate(fin):
                fout.write(line)
                if os.stat(fout.fileno()).st_size > self.max_size:
                    # close file before uploading
                    fout.close()
                    self.log.info(f"uploading file part {part_id}")
                    # upload data to mysql target table from file
                    self.uploadCSV(part_file, hook)
                    part_id += 1
                    # clear data for next loop operation
                    fout = open(part_file, "w")
            
            # handle the final chunk data of the file
            fout.close()

            if os.stat(part_file).st_size > 0:
                self.log.info(f'uploading final part of file')
                self.uploadCSV(part_file, hook)

    def uploadCSV(self, part_file, hook):
        # Check hook is MySqlHook or not
        if isinstance(hook, MySqlHook):
            self.log.info(f"Import csv file {part_file}")
            self.log.info(f"""
                LOAD DATA LOCAL INFILE '{part_file}'
                {self.duplicate_key_handling}
                INTO TABLE {self.destination_table}
                {self.extra_options}
            """)
            hook.bulk_load_custom(self.destination_table, part_file, self.duplicate_key_handling, self.extra_options)
        else:
            raise RuntimeError(
                f'The destination hook for connection {self.destination_conn_id!r} is not a instance of MySqlHook!'
                f"({type(hook).__name__}) has no `bulk_load_custom` method "
                )
