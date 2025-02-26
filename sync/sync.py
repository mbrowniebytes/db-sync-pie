import os

import orjson

from sync.sync_delete import SyncDelete
from sync.sync_insert import SyncInsert
from sync.sync_select import SyncSelect
from sync.sync_thread import SyncThread
from sync.sync_update import SyncUpdate
from utils import json


class Sync:
    def __init__(self, config, log, sql):
        self.config = config
        self.log = log
        self.sql = sql

        self.sync_thread = SyncThread(config, log)

        self.dry_run = "Dryrun: " if self.config.dry_run else ""

    def get_tables(self, type):
        sync_dir = os.path.dirname(__file__)
        main_dir = os.path.dirname(sync_dir)

        sync_tables_file = os.path.join(main_dir, f"sync_tables_{type}.json")

        if not os.path.isfile(sync_tables_file):
            raise ValueError(f"File not found {sync_tables_file}")

        sync_tables = "[]"
        with open(sync_tables_file, 'r') as file:
            sync_tables_json = file.read()
            sync_tables = orjson.loads(sync_tables_json)

        return sync_tables

    def insert(self, tables):
        sync_insert = SyncInsert(self.config, self.log, self.sql)

        self.log.info(f"{self.dry_run}insert new data: ",
                      f"Source: {self.config.db_source_name}:{self.config.db_source_dbname} -> "
                      f"Target: {self.config.db_target_name}:{self.config.db_target_dbname}"
                      )

        results = self.sync_thread.pool(tables, sync_insert.sync_insert, sync_insert.sync_done_callback)

        self.log.info(f"{self.dry_run}insert sync_thread.pool results ", results=results)
        return results

    def update(self, tables):
        sync_update = SyncUpdate(self.config, self.log, self.sql)

        self.log.info(f"{self.dry_run}update new data: ",
                      f"Source: {self.config.db_source_name}:{self.config.db_source_dbname} -> "
                      f"Target: {self.config.db_target_name}:{self.config.db_target_dbname}"
                      )

        results = self.sync_thread.pool(tables, sync_update.sync_update, sync_update.sync_done_callback)

        self.log.info(f"{self.dry_run}update sync_thread.pool results ", results=results)
        return results

    def delete(self, tables):
        sync_delete = SyncDelete(self.config, self.log, self.sql)

        self.log.info(f"{self.dry_run}delete deleted data: ",
                      f"Source: {self.config.db_source_name}:{self.config.db_source_dbname} -> "
                      f"Target: {self.config.db_target_name}:{self.config.db_target_dbname}"
                      )

        results = self.sync_thread.pool(tables, sync_delete.sync_delete, sync_delete.sync_done_callback)

        self.log.info(f"{self.dry_run}delete sync_thread.pool results ", results=results)
        return results

    def full(self):
        results = []

        tables = self.get_tables("insert")
        results.append(self.insert(tables))

        tables = self.get_tables("update")
        results.append(self.update(tables))

        tables = self.get_tables("delete")
        results.append(self.delete(tables))

        return results

    def show_tables(self):
        sync_select = SyncSelect(self.config, self.log, self.sql)

        self.log.info(f"{self.dry_run}show_tables: ",
                      f"Source: {self.config.db_source_name}:{self.config.db_source_dbname} -> "
                      f"Target: {self.config.db_target_name}:{self.config.db_target_dbname}"
                      )

        result = sync_select.select_show_tables()

        # print(f"created {result["file"]}, {result["size"]} bytes, {result["tables"]} tables")

        self.log.info(f"{self.dry_run}show_tables result ", result=result)
        return result


def main():
    print("not directly callable")


if __name__ == "__main__":
    main()
