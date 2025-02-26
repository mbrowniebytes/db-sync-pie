import logging
import os

from dotenv import load_dotenv

from utils.logger import Logger


class Config:
    def __init__(self):
        self.env = ""

        self.log_level = "DEBUG"
        self.log_extra_info = ""
        self.log_sql = False

        self.max_threads = 2

        self.db_primary_field = "id"

        self.db_update_limit_select = 10000
        self.db_update_modified_field = ""
        self.db_update_modified_from_date = "today"
        self.db_update_compare_method = "none"
        self.db_update_limit = 1

        self.db_insert_limit_select = 10000
        self.db_insert_batch_size = 1000
        self.db_insert_limit = 1

        self.db_delete_limit_select = 10000
        self.db_delete_limit = 1

        self.db_source_name = ""
        self.db_source_host = ""
        self.db_source_port = 3306
        self.db_source_dbname = ""
        self.db_source_user = ""
        self.db_source_password = ""
        self.db_source_file = ""

        self.db_engine = ""

        self.db_target_name = ""
        self.db_target_host = ""
        self.db_target_port = 3306
        self.db_target_dbname = ""
        self.db_target_user = ""
        self.db_target_password = ""
        self.db_target_file = ""

        self.batch_insert_row_size = 0

        self.dry_run = False

    def load_env(self):
        load_dotenv()

        self.env = os.getenv("ENV")

        self.log_level = os.getenv("LOG_LEVEL", logging.INFO)
        self.log_extra_info = os.getenv("LOG_EXTRA_INFO")
        # 1|True=log sql stmts
        self.log_sql = os.getenv("LOG_SQL", "False").lower() in ('true', '1', 't')

        # max cpu threads
        # nbr of processes (insert/update/delete) to run at once
        self.max_threads = int(os.getenv("MAX_THREADS", 2))

        # primary key, often id or code
        # if empty, specify in table structure
        self.db_primary_field = os.getenv("DB_PRIMARY_FIELD", "id")

        # update: max number of rows to select/modified for update per table
        # sql limit
        self.db_update_limit_select = int(os.getenv("DB_UPDATE_LIMIT_SELECT", 10000))
        # update: row last updated
        # timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
        # if empty, specify in table structure
        self.db_update_modified_field = os.getenv("DB_UPDATE_MODIFIED_FIELD")
        # update: dateparser date, update rows >= date
        self.db_update_modified_from_date = os.getenv("DB_UPDATE_MODIFIED_FROM_DATE", "today")
        # update: compare rows between source and target before update; "none"|"timestamp"
        self.db_update_compare_method = os.getenv("DB_UPDATE_COMPARE_METHOD")

        # update: max number of rows to update per table
        self.db_update_limit = int(os.getenv("DB_UPDATE_LIMIT", 1))

        # insert: max number of rows to select/primary for insert per table
        # sql limit; due to gaps in ids, may result in less actual rows
        self.db_insert_limit_select = int(os.getenv("DB_INSERT_LIMIT_SELECT", 10000))
        # insert: max rows to batch insert in one sql stmt
        self.db_insert_batch_size = int(os.getenv("DB_INSERT_BATCH_SIZE", 1000))
        # insert: max number of rows to insert per table
        self.db_insert_limit = int(os.getenv("DB_INSERT_LIMIT", 1))

        # delete: max number of rows to select/compare for delete per table
        self.db_delete_limit_select = int(os.getenv("DB_DELETE_LIMIT_SELECT", 10000))
        # delete: max number of rows to delete per table
        self.db_delete_limit = int(os.getenv("DB_DELETE_LIMIT", 1))

        # sqlite3|mysql|mariadb|postgres
        self.db_engine = os.getenv("DB_ENGINE")

        self.db_source_name = os.getenv("DB_SOURCE_NAME")
        self.db_source_host = os.getenv("DB_SOURCE_HOST")
        self.db_source_port = int(os.getenv("DB_SOURCE_PORT", 3306))
        self.db_source_dbname = os.getenv("DB_SOURCE_DBNAME")
        self.db_source_user = os.getenv("DB_SOURCE_USER")
        self.db_source_password = os.getenv("DB_SOURCE_PASSWORD")
        self.db_source_file = os.getenv("DB_SOURCE_FILE")

        self.db_target_name = os.getenv("DB_TARGET_NAME")
        self.db_target_host = os.getenv("DB_TARGET_HOST")
        self.db_target_port = int(os.getenv("DB_TARGET_PORT", 3306))
        self.db_target_dbname = os.getenv("DB_TARGET_DBNAME")
        self.db_target_user = os.getenv("DB_TARGET_USER")
        self.db_target_password = os.getenv("DB_TARGET_PASSWORD")
        self.db_target_file = os.getenv("DB_TARGET_FILE")


def main():
    config = Config()
    config.load_env()
    logger = Logger()
    log = logger.get_logger()
    attrs = vars(config)
    # log.info("config", properties=attrs)
    for item in attrs:
        log.info("config", property=item, value=attrs[item])


if __name__ == "__main__":
    main()
