import os
import random
import uuid

from utils.config import Config
from utils.logger import Logger
from utils.sql import Sql


class SetupTests:
    def __init__(self):
        self.config = Config()
        self.config.load_env()

        self.config.dry_run = False
        self.config.db_engine = "sqlite3"
        self.config.log_level = "WARNING"
        # self.config.log_level = "INFO" # for debug

        self.config.db_source_dbname = "test_source"
        self.config.db_target_dbname = "test_target"

        tests_dir = os.path.dirname(__file__)
        tests_sql_path = os.path.join(tests_dir, "sql")
        self.config.db_source_file = os.path.join(tests_sql_path, "test_source.db")
        self.config.db_target_file = os.path.join(tests_sql_path, "test_target.db")

        logger = Logger(self.config)
        self.log = logger.get_logger()

        self.sql = Sql(self.config, self.log)

    def get_setup(self):
        return self.config, self.log, self.sql

    def get_tables(self, type):
        tables = [
            {"name": "test_table_1", "modified_field": "modified"},
            {"name": "test_table_2", "modified_field": "modified"},
        ]
        return tables

    def create_schema_data(self, qty_test_rows):

        conn_source, cur_source = self.sql.connect_to_source()
        conn_target, cur_target = self.sql.connect_to_target()

        tests_dir = os.path.dirname(__file__)
        tests_sql_path = os.path.join(tests_dir, "sql")
        sql_file = os.path.join(tests_sql_path, "test.sql")

        rows_affected = self.sql.execute_script(sql_file=sql_file, cur=cur_source)
        self.log.info(f"Created Source schema using {rows_affected} stmts")

        rows_affected = self.sql.execute_script(sql_file=sql_file, cur=cur_target)
        self.log.info(f"Created Target schema using {rows_affected} stmts")

        self.add_test_data(cur_source, "test_table_1", {"name": "string", "address": "string"}, qty_test_rows)
        self.add_test_data(cur_source, "test_table_2", {"item": "string", "price": "decimal"}, qty_test_rows)

        conn_source.close()
        conn_target.close()

    def add_test_data(self, cur, table_name, sql_fields, iterations):

        # sql insert
        sql_field_names = ''
        sql_field_values = ''
        for sql_field in sql_fields:
            sql_field_names += f"{sql_field}, "
            # other sql
            # sql_field_values += "?, "
            # MySQLdb %s if row list, or %(field)s to match row dict
            sql_field_values += self.sql.get_param_placeholder(sql_field) + ", "

        sql_field_names = sql_field_names.rstrip(", ")
        sql_field_values = sql_field_values.rstrip(", ")

        # sql vary data
        rows = []
        for i in range(iterations):
            row = {}
            for sql_field, sql_field_type in sql_fields.items():
                match sql_field_type:
                    case "decimal":
                        value = i * random.randint(1, 1000) / 10
                    case "string" | _:
                        uid = str(uuid.uuid4()).split("-")[0]
                        value = f"{sql_field} {i} {uid}"
                row[sql_field] = value

            row = self.sql.get_param_values(row)
            rows.append(row)

        rows_affected = self.sql.execute_many(cur=cur,
                                              sql=f"INSERT INTO {table_name} "
                                                  f"({sql_field_names}) VALUES "
                                                  f"({sql_field_values})",
                                              params=rows
                                              )

        self.log.info(f"Inserted {rows_affected} rows into {table_name}")


def main():
    setup_tests = SetupTests()
    setup_tests.create_schema_data(100)


if __name__ == "__main__":
    main()
