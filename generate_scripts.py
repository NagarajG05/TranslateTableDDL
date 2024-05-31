# from sqlalchemy.dialects import snowflake
import os
import shutil
from datetime import datetime

import pandas as pd
from sqlalchemy import MetaData, Column, String, text, Table, TIMESTAMP, VARCHAR
from sqlalchemy.schema import CreateTable

from db_engine import generate_db_engine
from read_yml import YamlReader
from sql_queries import HANA_COLUMN_QRY


class TableMigration:
    invalid_types_mapping = {'SECONDDATE': TIMESTAMP,
                             'NCLOB': lambda: VARCHAR(16777216),
                             'CLOB': lambda: VARCHAR(16777216)
                             }

    def __init__(self, yaml_file_path, credential_path):
        self.schema_config = YamlReader.read_yaml(yaml_file_path)
        self.target = self.schema_config.get('target')
        self.table_types = self.schema_config.get('table_types', [])
        self.column_names = self.schema_config.get('column_names', 'uppercase')
        self.column_length = self.schema_config.get('column_length', 'current')
        self.schema_info_file = self.schema_config.get('source_file')
        self.result_file = self.schema_config.get('result_file')
        self.credential_config = YamlReader.read_yaml(credential_path)
        self.target_con_parms = self.credential_config[self.target]
        self.target_engine = generate_db_engine(self.target, self.target_con_parms)
        self.log_success_msgs = []
        self.log_error_msgs = []

        # Create the result directory
        timestamp = datetime.now().strftime("%Y%m%d")
        self.result_dir = os.path.join(self.result_file, f"{self.target}_TableScript_{timestamp}")
        if os.path.exists(self.result_dir):
            shutil.rmtree(self.result_dir)
        os.makedirs(self.result_dir)

    def generate_create_table_script(self):
        df = pd.read_excel(self.schema_info_file)

        for index, row in df.iterrows():
            try:
                source_db = row.get('SOURCE_DB', '').strip().lower() if row.get('SOURCE_DB', '') else ''
                source_schema = row.get('SOURCE_SCHEMA', '').strip()
                source_table = row.get('SOURCE_TABLE', '').strip()
                target_schema = row.get('TARGET_SCHEMA', '').strip()
                target_table = row.get('TARGET_TABLE', '').strip()
                disable = row.get('DISABLE', '').strip()
                build = row['BUILD'].strip() or 'N'

                if disable.upper() == 'N':
                    source_conn_parms = self.credential_config[source_db]
                    source_engine = generate_db_engine(source_db, source_conn_parms)

                    source_metadata = MetaData(bind=source_engine)
                    source_table_object = Table(source_table, source_metadata, autoload=True,
                                                autoload_with=source_engine,
                                                schema=source_schema)
                    source_select_query = ''
                    sourceselect_file_name = source_table.split('/')[-1]
                    if source_db == 'hana':
                        with source_engine.connect() as conn:
                            result = conn.execute(text(HANA_COLUMN_QRY), view_name=source_table,
                                                  schema_name=source_schema)
                            for row in result:
                                # Get the concatenated column names
                                col_names = row.col.split(',')

                                # Construct the new SELECT query using the retrieved column names
                                source_select_query = f'''SELECT {', '.join(col_names)} FROM "{source_schema}"."{source_table}" ;'''

                        select_query_file_path = os.path.join(self.result_dir, f"{sourceselect_file_name}_select.txt")
                        with open(select_query_file_path, 'w') as file:
                            file.write(source_select_query)
                            print(f'SELECT query is generated successfully for: {source_table}')

                    if not self.table_types:
                        self.generate_script_for_table_type(source_table_object, target_schema, target_table,
                                                            build_table=build)
                    else:
                        for table_type_name, table_type_properties in self.table_types.items():
                            prefix = table_type_properties.get('prefix', '')
                            suffix = table_type_properties.get('suffix', '')
                            default_cols_position = table_type_properties.get('default_cols_position', 'end')
                            default_cols = table_type_properties.get('default_cols', [])
                            if not isinstance(default_cols, list):
                                default_cols = [default_cols]
                            target_table_name = f"{prefix}{target_table}{suffix}"
                            self.generate_script_for_table_type(source_table_object, target_schema, target_table_name,
                                                                build_table=build,
                                                                default_cols=default_cols,
                                                                default_cols_position=default_cols_position)

                    print(f"DDL script is generated successfully for:  {source_table}")
            except Exception as e:
                error_msg = f'ERROR: generating script for {source_schema}.{source_table} failed: {e}'
                print(error_msg)
                self.log_error_msgs.append(error_msg)

        ## Log errors
        log_dir = os.path.join(self.result_dir, 'log')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        error_log_file_path = os.path.join(log_dir, "error_log.txt")
        # Write error messages to the log file
        with open(error_log_file_path, "w") as log_file:
            for error_message in self.log_error_msgs:
                log_file.write(error_message + "\n")

    def generate_script_for_table_type(self, source_table_metadata, target_schema, target_table_name,
                                       build_table: str = 'N',
                                       default_cols: list = None, default_cols_position: str = None):

        table_object = source_table_metadata.to_metadata(metadata=MetaData(), schema=target_schema,
                                                         name=target_table_name)

        new_columns = []
        for column in table_object.columns:
            new_col_name = column.name.upper() if self.column_names == 'uppercase' else column.name.lower() if self.column_names == 'lowercase' else column.name

            column_type_name = column.type.__class__.__name__
            if column_type_name in TableMigration.invalid_types_mapping:
                new_column = Column(new_col_name, TableMigration.invalid_types_mapping[column_type_name]())
            # if column.type.__class__.__name__ in TableMigration.invalid_types:
            # new_column = Column(new_col_name, TIMESTAMP())
            elif isinstance(column.type, String) and self.column_length != 'current':
                new_column = Column(new_col_name, column.type.__class__(length=self.column_length))
            else:
                new_column = Column(new_col_name, column.type)
            new_columns.append(new_column)

        target_metadata = MetaData(bind=self.target_engine)
        target_table_object = Table(target_table_name, target_metadata, *new_columns, schema=f'"{target_schema}"',
                                    extend_existing=True)
        create_table_sql = str(CreateTable(target_table_object).compile(dialect=self.target_engine.dialect))

        create_table_sql = create_table_sql.replace('CREATE TABLE ', 'CREATE OR REPLACE TABLE ')

        if default_cols:
            # Extract the column definitions from the generated SQL
            create_table_sql = create_table_sql.strip()
            column_definitions_start = create_table_sql.index('(') + 1
            column_definitions_end = create_table_sql.rindex(')')
            column_definitions = create_table_sql[column_definitions_start:column_definitions_end].strip()

            # Generate default column definitions as strings
            default_columns = ", ".join(default_cols)

            # Concatenate default columns at the specified position
            if default_cols_position == 'start':
                column_definitions = f"{default_columns}, {column_definitions}"
            else:
                column_definitions = f"{column_definitions}, {default_columns}"
            # Reconstruct the CREATE TABLE statement with default columns
            create_table_sql = f'CREATE OR REPLACE TABLE "{target_schema}"."{target_table_name}" ({column_definitions});'

        ## write DDL to TXT file
        script_file_path = os.path.join(self.result_dir, f"{target_table_name}_ddl.txt")
        with open(script_file_path, 'w') as file:
            file.write(create_table_sql)

        if build_table.upper() == 'Y':
            # Execute the CREATE TABLE statement in the target database
            with self.target_engine.connect() as connection:

                try:
                    connection.execute(create_table_sql)
                    print(f'Success: {target_table_name} table created successfully')
                except Exception as e:
                    error_msg = 'Error: Creating table failed due to {}'.format(e)
                    print(error_msg)
                    self.log_error_msgs.append(error_msg)

## Testing
# migrate = TableMigration("schema_config.yml", "credentials.yml")
# migrate.generate_create_table_script()
# print('end')
