import os
import re
import time
from typing import LiteralString

import sqlite3
import MySQLdb
from MySQLdb.cursors import SSDictCursor
import psycopg2
import psycopg2.extras
import mariadb
import mysql.connector



class Sql:
    def __init__(self, config, log):
        self.config = config
        self.log = log

        self.log_sql = self.config.log_sql
        self.param_style = "?"

    def _log(self, prefix, data, nbr_rows=None, start=0.0):
        if not self.log_sql:
            return
        msg = f"{prefix}: {data}: "
        if nbr_rows is not None and nbr_rows != -1:
            msg += f"{nbr_rows} rows "
        if start:
            finish = time.perf_counter()
            elapsed = round(finish - start, 2)
            msg += f"in {elapsed}s"

        self.log.info(msg)

    def _log_connect(self, host, dbname, start):
        self._log("SQL connected", {"host": host, "dbname": dbname}, start)

    def _log_connect_file(self, db_file, start):
        self._log("SQL connected", {"db_file": db_file}, start)

    def _log_select(self, sql, params, nbr_rows=None, start=0.0):
        # TODO sanitize common pwd params
        self._log("SQL select", {"sql": sql, "params": params}, nbr_rows, start)

    def _log_execute(self, sql, params, nbr_rows, start):
        # TODO sanitize common pwd params
        self._log("SQL execute", {"sql": sql, "params": params}, nbr_rows, start)

    def _connect(self, host, port, dbname, user, password, db_file):
        match self.config.db_engine:
            case "sqlite3":
                return self._connect_sqlite3_pkg(db_file)
            case "mysql" | "mariadb":
                return self._connect_mysqldb_pkg(
                    host=host,
                    port=port,
                    dbname=dbname,
                    user=user,
                    password=password,
                )
            case "postgres":
                return self._connect_psycopg2_pkg(
                    host=host,
                    port=port,
                    dbname=dbname,
                    user=user,
                    password=password,
                )
            case _:
                raise NotImplementedError(f"_connect: Unknown database engine: {self.config.db_engine}")

    def _sqlite3_dict_factory(self, cur, row):
        d = {}
        for idx, col in enumerate(cur.description):
            d[col[0]] = row[idx]
        return d

    def _connect_sqlite3_pkg(self, db_file):
        # used for testing
        start = time.perf_counter()
        try:
            conn = sqlite3.connect(db_file)

            # return dict instead of tuple
            # conn.row_factory = sqlite3.Row
            conn.row_factory = self._sqlite3_dict_factory

            cur = conn.cursor()

            self.param_style = ":"

            self._log_connect_file(db_file, start)
            return conn, cur
        except Exception as e:
            self.log.error(f"_connect database.Error:", e=e)
            raise Exception(e)

    # https://pypi.org/project/mysqlclient/#files
    # https://mysqlclient.readthedocs.io/
    def _connect_mysqldb_pkg(self, host, port, dbname, user, password):
        # server side cursors
        start = time.perf_counter()
        try:
            conn = MySQLdb.connect(
                host=host,
                port=port,
                database=dbname,
                user=user,
                password=password,
                compress=True,
                cursorclass=SSDictCursor,
            )

            cur = conn.cursor()

            self.param_style = "%s"

            self._log_connect(host, dbname, start)
            return conn, cur
        except Exception as e:
            self.log.error(f"_connect database.Error:", e=e)
            raise Exception(e)

    def _connect_mysql_pkg(self, host, port, dbname, user, password):
        # may remove later
        try:
            conn = mysql.connector.connect(
                host=host,
                port=port,
                database=dbname,
                user=user,
                password=password,
            )

            cur = conn.cursor(buffered=True, dictionary=True, compress=True)

            self.param_style = "?"

            return conn, cur
        except Exception as e:
            self.log.error(f"_connect database.Error:", e=e)
            raise Exception(e)

    def _connect_mariadb_pkg(self, host, port, dbname, user, password):
        # may remove later
        try:
            conn = mariadb.connect(
                host=host,
                port=port,
                database=dbname,
                user=user,
                password=password,
            )

            cur = conn.cursor(buffered=True, dictionary=True, compress=True)

            self.param_style = "?"

            return conn, cur
        except Exception as e:
            self.log.error(f"_connect database.Error:", e=e)
            raise Exception(e)

    def _connect_psycopg2_pkg(self, host, port, dbname, user, password):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=dbname,
                user=user,
                password=password,
            )

            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            self.param_style = "%"

            return conn, cur
        except Exception as e:
            self.log.error(f"_connect database.Error:", e=e)
            raise Exception(e)

    def connect_to_source(self):
        return self._connect(
            host=self.config.db_source_host,
            port=self.config.db_source_port,
            dbname=self.config.db_source_dbname,
            user=self.config.db_source_user,
            password=self.config.db_source_password,
            db_file=self.config.db_source_file
        )

    def connect_to_target(self):
        return self._connect(
            host=self.config.db_target_host,
            port=self.config.db_target_port,
            dbname=self.config.db_target_dbname,
            user=self.config.db_target_user,
            password=self.config.db_target_password,
            db_file=self.config.db_target_file
        )

    def get_param_style(self, param_type):
        match self.param_style:
            case "?":
                param_style = "?"
            case ":":
                match param_type:
                    case "name":
                        param_style = ":"
                    case "position" | _:
                        param_style = "?"
            case "%s" | _:
                param_style = "%s"
        return param_style

    def get_param_placeholder(self, field_name):
        match self.param_style:
            case "?":
                param_placeholder = f"?"
            case ":":
                param_placeholder = f":{field_name}"
            case "%s" | _:
                param_placeholder = f"%({field_name})s"
        return param_placeholder

    def get_param_values(self, values):
        match self.param_style:
            case "?":
                # dict -> list of values
                values = values.values()
            # case ":" | "%s" | _:
        return values

    def select_one_row(self, cur, sql, params=(), assert_result=False,
            error_msg=None):
        start = time.perf_counter()
        try:
            cur.execute(sql, params)
            row = cur.fetchone()

            nbr_rows = cur.rowcount
            self._log_select(sql, params, nbr_rows, start)

            if assert_result and nbr_rows == 0:
                if error_msg:
                    msg = error_msg
                else:
                    msg = f"No results found for {sql}"
                self.log.error(msg)
                raise ValueError(msg)
            return row
        except Exception as e:
            self.log.error("select_one_row: exception", e=e)
            raise Exception(e)

    def select_all_rows(self, cur, sql, params=(), assert_result=False,
            error_msg=None):
        start = time.perf_counter()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
            nbr_rows = cur.rowcount
            self._log_select(sql, params, nbr_rows, start)

            if assert_result and nbr_rows == 0:
                if error_msg:
                    msg = error_msg
                else:
                    msg = f"No results found for {sql}"
                self.log.error(msg)
                raise ValueError(msg)
            return rows
        except Exception as e:
            self.log.error("select_all_rows: exception", e=e)
            raise Exception(e)

    def select(self, cur, sql, params=()):
        try:
            cur.execute(sql, params)
            self._log_select(sql, params)
            return cur
        except Exception as e:
            self.log.error("select: exception", e=e)
            raise Exception(e)

    def execute(self, cur, sql, params=()):
        start = time.perf_counter()
        try:
            if not self.config.dry_run:
                rows_affected = cur.execute(sql, params)
                if self.config.db_engine == "sqlite3":
                    cur.connection.commit()
            else:
                sql = f"Dryrun: {sql}"
                rows_affected = 0

            match self.config.db_engine:
                case "sqlite3":
                    # sqlite returns -1 for DROP|CREATE even if succeeds, return 1 instead
                    if hasattr(rows_affected, "rowcount"):
                        if rows_affected.rowcount == -1 and any(re.findall(r'DROP|CREATE', sql, re.IGNORECASE)):
                            rows_affected = 1
                        else:
                            rows_affected = rows_affected.rowcount
                # case _: rows_affected
            self._log_execute(sql, params, rows_affected, start)
            return rows_affected
        except Exception as e:
            self.log.error("execute: exception", e=e)
            raise Exception(e)

    def execute_many(self, cur, sql, params=()):
        start = time.perf_counter()
        nbr_params = len(params)
        if nbr_params == 0:
            msg = "execute_many without any values"
            self.log.error(msg)
            raise ValueError(msg)
        try:
            if not self.config.dry_run:
                rows_affected = cur.executemany(sql, params)

                if self.config.db_engine == "sqlite3":
                    cur.connection.commit()
            else:
                sql = f"Dryrun: {sql}"
                rows_affected = 0
            self._log_execute(sql, nbr_params, rows_affected, start)
            match self.config.db_engine:
                case "sqlite3":
                    if isinstance(rows_affected, int):
                        return rows_affected
                    else:
                        return rows_affected.rowcount
                case _:
                    return rows_affected
        except Exception as e:
            self.log.error("execute_many: exception", e=e)
            raise Exception(e)

    def execute_script(self, sql_file: LiteralString | str, cur: SSDictCursor):
        start = time.perf_counter()

        if not os.path.isfile(sql_file):
            raise ValueError(f"execute_script: sql_file {sql_file} not found")

        fh = open(sql_file, 'r')
        sql_contents = fh.read()
        fh.close()

        sql_commands = sql_contents.split(';')
        nbr_sql_commands = len(sql_commands)

        total_rows_affected = 0
        for sql_command in sql_commands:
            # $$ end of sql stmt; for triggers, nested BEGIN/END
            sql_command = sql_command.replace('$$', ';')
            rows_affected = self.execute(cur=cur,
                                         sql=sql_command
                                         )
            total_rows_affected += rows_affected

        self._log("SQL file executed", {"sql_file": sql_file, "nbr_sql_commands": nbr_sql_commands}, start)
        return total_rows_affected


def main():
    print("not directly callable")


if __name__ == "__main__":
    main()
