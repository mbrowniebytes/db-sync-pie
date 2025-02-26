import argparse

from sync.sync import Sync
from tests.setup_tests import SetupTests
from utils.config import Config
from utils.logger import Logger
from utils.sql import Sql


def main():
    parser = argparse.ArgumentParser(description="Sync data from source db to target db")
    parser.add_argument("--sync", choices=["full", "insert", "update", "delete"],
                        help="Sync method: insert, update, delete, full = insert, update, then delete"
                        )
    parser.add_argument("--select", choices=["show_tables"],
                        help="Select method: show_tables"
                        )
    parser.add_argument("--dryrun", action="store_true",
                        help="Run Selects, but not Inserts, Updates, Deletes; alias for --test"
                        )
    parser.add_argument("--test", action="store_true",
                        help="Run Selects, but not Inserts, Updates, Deletes; alias for --dryrun"
                        )
    # option toggled true if passed in, false if not
    # parser.add_argument("--opt", action="store_true", help="True")
    args = parser.parse_args()
    sync_method = args.sync
    dry_run = args.dryrun
    # common alias
    test = args.test
    if test:
        dry_run = True

    # adhoc dev testing using tests
    dev_test = False
    if dev_test:
        dry_run = False
        setup_tests = SetupTests()
        config, log, sql = setup_tests.get_setup()
        config.dry_run = dry_run

        tables = setup_tests.get_tables("all")
        tables = [
            {"name": "test_table_1", "modified_field": "modified"},
        ]

        logger = Logger(config)
        log = logger.get_logger("INFO")

        sync = Sync(config, log, sql)
        # results = sync.insert(tables)
        # results = sync.update(tables)
        sync.show_tables()
        exit(0)

    # adhoc dev testing using .env
    dev_run = False
    if dev_run:
        config = Config()
        config.load_env()
        config.dry_run = dry_run

        logger = Logger(config)
        log = logger.get_logger()

        sql = Sql(config, log)

        sync = Sync(config, log, sql)

        # tables = sync.get_tables("insert")
        # sync.insert(tables)
        sync.show_tables()
        exit(0)

    # main
    if sync_method is None:
        parser.print_help()
        exit(0)

    config = Config()
    config.load_env()
    config.dry_run = dry_run

    logger = Logger(config)
    log = logger.get_logger()

    sql = Sql(config, log)

    sync = Sync(config, log, sql)

    match sync_method:
        case "full":
            sync.full()
        case "insert":
            tables = sync.get_tables("insert")
            sync.insert(tables)
        case "update":
            tables = sync.get_tables("update")
            sync.update(tables)
        case "delete":
            tables = sync.get_tables("delete")
            sync.delete(tables)
        case "show_tables":
            sync.show_tables()


if __name__ == "__main__":
    main()
