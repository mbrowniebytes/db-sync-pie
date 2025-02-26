import time

import dateparser

from sync.sync_thread import SyncThread


class SyncUpdate:
    def __init__(self, config, log, sql):
        self.config = config
        self.log = log
        self.sql = sql

        self.sync_thread = SyncThread(config, log)

        self.dry_run = "Dryrun: " if self.config.dry_run else ""

    def sync_update(self, table):
        self.log.debug("_sync_update")
        time.sleep(2)

        table_name = table["name"]
        task_id = table["task_id"]

        # for table in tables:
        conn_source, cur_source = self.sql.connect_to_source()
        conn_target, cur_target = self.sql.connect_to_target()

        self.log.info(f"Table: {table_name}")

        # get updated data
        if "modified_field" in table:
            modified_field = table["modified_field"]
        else:
            modified_field = self.config.db_modified_field
        if not modified_field:
            msg = f"Missing required modified field for {table_name}"
            self.log.error(msg)
            raise ValueError(msg)
        if "modified_from_date" in table:
            modified_from_date = table["modified_from_date"]
        else:
            modified_from_date = self.config.db_update_modified_from_date
        if not modified_from_date:
            msg = f"Missing required modified from date for {table_name}"
            self.log.error(msg)
            raise ValueError(msg)
        # update using primary field
        if "primary_field" in table:
            primary_field = table["primary_field"]
        else:
            primary_field = self.config.db_primary_field
        if not primary_field:
            msg = f"Missing required primary field for {table_name}"
            self.log.error(msg)
            raise ValueError(msg)

        db_update_limit_select = self.config.db_update_limit_select
        modified_from_datetime = dateparser.parse(modified_from_date).isoformat(" ")
        db_update_limit = self.config.db_update_limit
        db_update_compare_method = self.config.db_update_compare_method

        # dict -> %(field)s or [array] -> %s
        # MySQLdb ? instead of %s = Exception: not all arguments converted during bytes formatting
        sql_modified_from_datetime = self.sql.get_param_placeholder("modified_from_datetime")
        params = self.sql.get_param_values({"modified_from_datetime": modified_from_datetime})

        row = self.sql.select_one_row(cur=cur_source,
                                      sql=f"SELECT COUNT({modified_field}) AS nbr_source_rows FROM {table_name} WHERE "
                                          f"{modified_field} >= {sql_modified_from_datetime}",
                                      params=params, assert_result=True,
                                      error_msg=f"Unable to determine Source Number of Rows to update for {table_name}"
                                      )
        nbr_source_rows = row["nbr_source_rows"]

        self.log.info(f"{self.dry_run}Table: {table_name}: "
                      f"Source {modified_field} >= {modified_from_datetime}, "
                      f"Number of Source Rows available to update into Target: {nbr_source_rows}, "
                      f"Max number of Source Rows to update into Target: {db_update_limit_select}"
                      )

        param_style = self.sql.get_param_style("position")

        cur_source = self.sql.select(cur=cur_source,
                                     sql=f"SELECT * FROM {table_name} "
                                         f"WHERE {modified_field} >= {param_style} ORDER BY {primary_field} LIMIT {param_style}",
                                     params=(modified_from_datetime, db_update_limit_select,)
                                     )

        sql_field_names = []
        total_rows_affected = 0
        sql = ""
        for index, row in enumerate(cur_source):

            if index == 0:
                # build update sql
                for key, value in row.items():
                    sql_field_names.append(key)

                sql = f"UPDATE {table_name} SET "
                for sql_field_name in sql_field_names:
                    if sql_field_name == primary_field:
                        continue
                    # other sql
                    # sql_field_values += "?, "
                    # MySQLdb %s if row list, or %(field)s to match row dict
                    sql += f"{sql_field_name} = "
                    sql += self.sql.get_param_placeholder(sql_field_name) + ", "

                sql = sql.rstrip(", ") + " "
                sql = sql + f"WHERE {primary_field} = "
                sql = sql + self.sql.get_param_placeholder(primary_field)


            if db_update_compare_method == "timestamp":
                # if timestamp/modified same between source and target, skip
                target_row = self.sql.select_one_row(cur=cur_target,
                                         sql=f"SELECT {modified_field} FROM {table_name} "
                                             f"WHERE {primary_field} = {param_style}",
                                         params=(row[primary_field],)
                                         )
                if target_row == row[modified_field]:
                    continue

            row = self.sql.get_param_values(row)

            # depends on db_engine, but if update is sent and no changes are required, rows_affected = 0
            rows_affected = self.sql.execute(cur=cur_target,
                                             sql=sql,
                                             params=row
                                             )

            total_rows_affected += rows_affected

            if total_rows_affected >= db_update_limit:
                self.log.info(f"{self.dry_run}reached update limit of {db_update_limit} rows in {table_name}")
                break

        # end for index, row in enumerate(self.cur_source):

        conn_source.close()
        conn_target.close()

        self.log.info(f"{self.dry_run}Table: {table_name}: "
                      f"done, updated {total_rows_affected} rows"
                      )

        return {"name": table_name, "nbr_rows": total_rows_affected, "task_id": task_id}

    def sync_done_callback(self, future):
        # no need for yet
        return
        self.log.info(f"sync_done_callback")
        try:
            result = future.result()
            self.log.info(f"sync_done_callback new data ", result=result)
        except Exception as e:
            self.log.error(f"sync_done_callback Exception", e=e)
            raise ValueError(e)


def main():
    print("not directly callable")


if __name__ == "__main__":
    main()
