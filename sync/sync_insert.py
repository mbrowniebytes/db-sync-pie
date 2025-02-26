import time

from sync.sync_thread import SyncThread


class SyncInsert:
    def __init__(self, config, log, sql):
        self.config = config
        self.log = log
        self.sql = sql

        self.sync_thread = SyncThread(config, log)

        self.dry_run = "Dryrun: " if self.config.dry_run else ""

    def sync_insert(self, table):
        self.log.debug("sync_insert")
        time.sleep(2)

        table_name = table["name"]
        task_id = table["task_id"]

        # for table in tables:
        conn_source, cur_source = self.sql.connect_to_source()
        conn_target, cur_target = self.sql.connect_to_target()

        self.log.info(f"Table: {table_name}")

        # get new data
        if "primary_field" in table:
            primary_field = table["primary_field"]
        else:
            primary_field = self.config.db_primary_field
        if not primary_field:
            msg = f"Missing required primary field for {table_name}"
            self.log.error(msg)
            raise ValueError(msg)

        db_insert_batch_size = self.config.db_insert_batch_size
        db_insert_limit_select = self.config.db_insert_limit_select
        db_insert_limit = self.config.db_insert_limit

        row = self.sql.select_one_row(cur=cur_source,
                                      sql=f"SELECT MAX({primary_field}) AS max_source_id FROM {table_name}",
                                      params=(), assert_result=True,
                                      error_msg=f"Unable to determine Source MAX({primary_field}) for {table_name}"
                                      )
        max_source_id = row["max_source_id"]
        if max_source_id is None:
            # null|None = no rows
            max_source_id = 0

        row = self.sql.select_one_row(cur=cur_target,
                                      sql=f"SELECT MAX({primary_field}) AS max_target_id FROM {table_name}",
                                      params=(), assert_result=True,
                                      error_msg=f"Unable to determine Target MAX({primary_field}) for {table_name}"
                                      )
        max_target_id = row["max_target_id"]
        if max_target_id is None:
            # null|None = no rows
            max_target_id = 0

        # dict -> %(field)s or [array] -> %s
        # MySQLdb ? instead of %s = Exception: not all arguments converted during bytes formatting
        sql_max_target_id = self.sql.get_param_placeholder("max_target_id")
        params = self.sql.get_param_values({"max_target_id": max_target_id})

        row = self.sql.select_one_row(cur=cur_source,
                                      sql=f"SELECT COUNT({primary_field}) AS nbr_source_rows FROM {table_name} WHERE "
                                          f"{primary_field} > {sql_max_target_id}",
                                      params=params, assert_result=True,
                                      error_msg=f"Unable to determine Source Number of Rows to Insert for {table_name}"
                                      )
        nbr_source_rows = row["nbr_source_rows"]

        self.log.info(f"{self.dry_run}Table: {table_name}: "
                      f"Source Max {primary_field}: {max_source_id}, "
                      f"Target Max {primary_field}: {max_target_id}, "
                      f"Number of Source Rows to Insert into Target: {nbr_source_rows}, "
                      f"Max number of Source Rows to Insert into Target: {db_insert_limit_select}"
                      )

        param_style = self.sql.get_param_style("position")

        cur_source = self.sql.select(cur=cur_source,
                                     sql=f"SELECT * FROM {table_name} "
                                         f"WHERE {primary_field} > {param_style} ORDER BY {primary_field} LIMIT {param_style} ",
                                     params=(max_target_id, db_insert_limit_select,)
                                     )

        rows = []
        sql_field_names = ""
        sql_field_values = ""
        row_nbr = 0
        total_rows_affected = 0
        for index, row in enumerate(cur_source):
            msg = ""
            row_nbr += 1

            if index == 0:
                for field, value in row.items():
                    msg += f"{field}: {value}, "
                    sql_field_names += f"{field}, "
                    # other sql
                    # sql_field_values += "?, "
                    # MySQLdb %s if row list, or %(field)s to match row dict
                    sql_field_values += self.sql.get_param_placeholder(field) + ", "

                sql_field_names = sql_field_names.rstrip(", ")
                sql_field_values = sql_field_values.rstrip(", ")

            rows.append(row)

            if row_nbr >= db_insert_limit:
                self.log.info(f"{self.dry_run}reached insert limit of {db_insert_limit} rows into {table_name}")
                break

            if row_nbr % db_insert_batch_size == 0:
                nbr_rows = len(rows)
                self.log.info(f"{self.dry_run}inserting batch {nbr_rows} rows into {table_name}")
                rows_affected = self.sql.execute_many(cur=cur_target,
                                                      sql=f"INSERT INTO {table_name} ({sql_field_names}) VALUES ({sql_field_values})",
                                                      params=rows
                                                      )
                total_rows_affected += rows_affected
                # reset batch
                rows = []
        # end for index, row in enumerate(self.cur_source):

        # process remaining rows
        nbr_rows = len(rows)
        if nbr_rows > 0:
            self.log.info(f"{self.dry_run}inserting remaining batch {nbr_rows} rows into {table_name}")
            rows_affected = self.sql.execute_many(cur=cur_target,
                                                  sql=f"INSERT INTO {table_name} ({sql_field_names}) VALUES ({sql_field_values})",
                                                  params=rows
                                                  )
            total_rows_affected += rows_affected

        conn_source.close()
        conn_target.close()

        self.log.info(f"{self.dry_run}Table: {table_name}: "
                      f"done, inserted {total_rows_affected} rows"
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
