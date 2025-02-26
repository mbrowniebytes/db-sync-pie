import time
from math import ceil

from sync.sync_thread import SyncThread


class SyncDelete:
    def __init__(self, config, log, sql):
        self.config = config
        self.log = log
        self.sql = sql

        self.sync_thread = SyncThread(config, log)

        self.dry_run = "Dryrun: " if self.config.dry_run else ""

    def sync_delete(self, table):
        self.log.debug("_sync_delete")
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

        db_delete_limit_select = self.config.db_delete_limit_select
        db_delete_limit = self.config.db_delete_limit

        row = self.sql.select_one_row(cur=cur_target,
                                      sql=f"SELECT MAX({primary_field}) AS max_target_id FROM {table_name}",
                                      params=(), assert_result=True,
                                      error_msg=f"Unable to determine Target MAX({primary_field}) for {table_name}"
                                      )
        max_target_id = row["max_target_id"]

        self.log.info(f"{self.dry_run}Table: {table_name}: "
                      f"Target Max {primary_field}: {max_target_id}, "
                      f"Max number of Source Rows to Delete from Target: {db_delete_limit_select}"
                      )

        param_style = self.sql.get_param_style("position")
        total_nbr_rows_deleted = 0
        nbr_batches = ceil(max_target_id / db_delete_limit_select)
        for batch_nbr in range(nbr_batches):

            start_id = batch_nbr * db_delete_limit_select + 1
            end_id = start_id + db_delete_limit_select

            cur_source = self.sql.select(cur=cur_source,
                                         sql=f"SELECT {primary_field} FROM {table_name} "
                                             f"WHERE {primary_field} BETWEEN {param_style} AND {param_style}",
                                         params=(start_id, end_id,)
                                         )
            source_ids = []
            for index, row in enumerate(cur_source):
                source_ids.append(row[primary_field])

            cur_target = self.sql.select(cur=cur_target,
                                         sql=f"SELECT {primary_field} FROM {table_name} "
                                             f"WHERE {primary_field} BETWEEN {param_style} AND {param_style}",
                                         params=(start_id, end_id,)
                                         )
            target_ids = []
            for index, row in enumerate(cur_target):
                target_ids.append(row[primary_field])

            target_ids_to_delete = [item for item in target_ids if item not in source_ids]
            nbr_target_to_delete = len(target_ids_to_delete)

            msg = (f"{self.dry_run}Table: {table_name}: ",
                   f"found {nbr_target_to_delete} rows to delete")
            if nbr_target_to_delete >= db_delete_limit:
                msg += f", limiting to {db_delete_limit} rows"
                target_ids_to_delete = target_ids_to_delete[:db_delete_limit]
            self.log.info(msg)

            sql_ids_params = ','.join([param_style] * len(target_ids_to_delete))

            nbr_rows_deleted = self.sql.execute(cur=cur_target,
                                                sql=f"DELETE FROM {table_name} "
                                                    f"WHERE {primary_field} IN ({sql_ids_params})",
                                                params=target_ids_to_delete
                                                )
            self.log.info(f"{self.dry_run}Table: {table_name}: "
                          f"deleted {nbr_target_to_delete} rows"
                          )

            total_nbr_rows_deleted += nbr_rows_deleted
            if nbr_target_to_delete >= db_delete_limit:
                self.log.info(f"{self.dry_run}reached delete limit of {db_delete_limit} rows in {table_name}")
                break
        # end for batch_nbr in range(nbr_batches):

        self.log.info(f"{self.dry_run}Table: {table_name}: "
                      f"done, deleted {total_nbr_rows_deleted} rows"
                      )

        conn_source.close()
        conn_target.close()

        return {"name": table_name, "nbr_rows": total_nbr_rows_deleted, "task_id": task_id}

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
