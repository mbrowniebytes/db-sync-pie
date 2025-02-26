import os
import orjson



class SyncSelect:
    def __init__(self, config, log, sql):
        self.config = config
        self.log = log
        self.sql = sql

        self.dry_run = "Dryrun: " if self.config.dry_run else ""

    def select_show_tables(self):
        self.log.debug("_select_show_tables")

        conn_source, cur_source = self.sql.connect_to_source()
        # conn_target, cur_target = self.sql.connect_to_target()


        db_engine = self.config.db_engine
        db_source_dbname = self.config.db_source_dbname
        # db_target_dbname = self.config.db_target_dbname

        match db_engine:
            case "sqlite3":
                sql = (f"SELECT name FROM sqlite_master "
                       f"WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            case "mysql" | "mariadb":
                sql = (f"SELECT TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES "
                       f"WHERE TABLE_SCHEMA = '{db_source_dbname}'")
            case "postgres":
                sql = (f"SELECT table_name AS name FROM pg_catalog.pg_tables "
                       f"WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')")
            case _:
                raise NotImplementedError(f"select_show_tables: {db_engine} not implemented")

        rows = self.sql.select_all_rows(cur=cur_source,
                                      sql=sql,
                                      params=(), assert_result=True,
                                      error_msg=f"Unable to select show tables"
                                      )
        tables = []
        for row in rows:
            table = {"name": row["name"]}
            tables.append(table)

        tables.sort(key=lambda x: x["name"])
        nbr_tables = len(tables)


        tables_json = orjson.dumps(tables).decode("utf8")
        # option=orjson.OPT_APPEND_NEWLINE|orjson.OPT_INDENT_2 formats 'too much'
        # format json table per line
        tables_json = tables_json.replace("[{", "[\n    {")
        tables_json = tables_json.replace("},", "},\n    ")
        tables_json = tables_json.replace("}]", "}\n]")

        sync_dir = os.path.dirname(__file__)
        main_dir = os.path.dirname(sync_dir)
        tables_json_file = os.path.join(main_dir, "sync_tables_show_tables.json")
        with open(tables_json_file, "w") as f:
            f.write(tables_json)

        tables_json_filesize = os.path.getsize(tables_json_file)

        self.log.info(f"{self.dry_run}Show tables "
                      f"done, created {tables_json_file}, {tables_json_filesize} bytes, {nbr_tables} tables"
                      )

        conn_source.close()
        # conn_target.close()

        return {"file": tables_json_file, "size": tables_json_filesize, "tables": nbr_tables}


def main():
    print("not directly callable")


if __name__ == "__main__":
    main()
