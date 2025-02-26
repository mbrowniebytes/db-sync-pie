from sync.sync import Sync
from tests.setup_tests import SetupTests


#
# pipenv run pytest -c tests/pytest.ini  -v
# https://docs.pytest.org/en/stable/
#

# @pytest.fixture
# def sync():
#    setup_tests = SetupTests()
#    qty_test_rows = 100
#    setup_tests.create_schema_data(qty_test_rows)
#    config, log, sql = setup_tests.get_setup()
#
#    sync = Sync(config, log, sql)
#    return sync

def test_start():
    assert 1 == 1


def test_setup():
    setup_tests = SetupTests()
    qty_test_rows = 100
    setup_tests.create_schema_data(qty_test_rows)
    config, log, sql = setup_tests.get_setup()
    sync = Sync(config, log, sql)

    assert 1 == 1

def test_show_tables():
    setup_tests = SetupTests()
    config, log, sql = setup_tests.get_setup()
    sync = Sync(config, log, sql)

    result = sync.show_tables()

    assert result["tables"] == 2
    assert result["file"].find("sync_tables_show_tables.json") != -1


def test_insert_first():
    setup_tests = SetupTests()
    qty_test_rows = 100
    config, log, sql = setup_tests.get_setup()
    config.db_insert_limit_select = 10
    config.db_insert_limit = 10
    sync = Sync(config, log, sql)

    conn_source, cur_source = sync.sql.connect_to_source()
    conn_target, cur_target = sync.sql.connect_to_target()

    table_name = "test_table_1"
    tables = [
        {"name": "test_table_1", "modified_field": "modified"},
    ]

    for table in tables:
        table_name = table["name"]

        row_source = sync.sql.select_one_row(cur=cur_source,
                                             sql=f"SELECT COUNT(id) AS nbr_source_rows FROM {table_name}",
                                             params=(), assert_result=True,
                                             error_msg=f"Unable to determine Source Number of Rows for {table_name}"
                                             )
        assert row_source["nbr_source_rows"] > 0
        assert row_source["nbr_source_rows"] == qty_test_rows

        row_target = sync.sql.select_one_row(cur=cur_target,
                                             sql=f"SELECT COUNT(id) AS nbr_target_rows FROM {table_name}",
                                             params=(), assert_result=True,
                                             error_msg=f"Unable to determine Target Number of Rows for {table_name}"
                                             )
        assert row_target["nbr_target_rows"] == 0

    results = sync.insert(tables)
    # [{'name': 'test_table_1', 'nbr_rows': 10, 'task_id': 1}]
    for result in results:
        assert result["nbr_rows"] == config.db_insert_limit_select

    for table in tables:
        table_name = table["name"]

        row_target = sync.sql.select_one_row(cur=cur_target,
                                             sql=f"SELECT COUNT(id) AS nbr_target_rows FROM {table_name}",
                                             params=(), assert_result=True,
                                             error_msg=f"Unable to determine Target Number of Rows for {table_name}"
                                             )
        assert row_target["nbr_target_rows"] > 0
        assert row_target["nbr_target_rows"] == config.db_insert_limit_select

    conn_source.close()
    conn_target.close()


def test_insert_second():
    setup_tests = SetupTests()
    qty_test_rows = 100
    config, log, sql = setup_tests.get_setup()
    config.db_insert_limit_select = 10
    config.db_insert_limit = 10
    sync = Sync(config, log, sql)

    conn_source, cur_source = sync.sql.connect_to_source()
    conn_target, cur_target = sync.sql.connect_to_target()

    tables = [
        {"name": "test_table_1", "modified_field": "modified"},
    ]

    results = sync.insert(tables)
    # [{'name': 'test_table_1', 'nbr_rows': 10, 'task_id': 1}]
    for result in results:
        assert result["nbr_rows"] == config.db_insert_limit

    for table in tables:
        table_name = table["name"]

        row_target = sync.sql.select_one_row(cur=cur_target,
                                             sql=f"SELECT COUNT(id) AS nbr_target_rows FROM {table_name}",
                                             params=(), assert_result=True,
                                             error_msg=f"Unable to determine Target Number of Rows for {table_name}"
                                             )
        assert row_target["nbr_target_rows"] > 0
        nbr_target_rows = min(config.db_insert_limit * 2, qty_test_rows)
        assert row_target["nbr_target_rows"] == nbr_target_rows

    conn_source.close()
    conn_target.close()


def test_update():
    setup_tests = SetupTests()
    qty_test_rows = 100
    config, log, sql = setup_tests.get_setup()
    config.db_update_limit_select = 10
    config.db_update_modified_from_date = "1 hour ago utc"
    config.db_update_limit = 10
    sync = Sync(config, log, sql)

    conn_source, cur_source = sync.sql.connect_to_source()
    conn_target, cur_target = sync.sql.connect_to_target()

    table_name = "test_table_1"
    tables = [
        {"name": "test_table_1", "modified_field": "modified"},
    ]

    # update rows
    update_row_ids = config.db_update_limit_select * 2
    nbr_update_row_ids = update_row_ids
    for table in tables:
        table_name = table["name"]
        primary_field = config.db_primary_field

        match table_name:
            case "test_table_1":
                sql = (f"UPDATE {table_name} SET "
                       f"name = CONCAT(name, ' test update') "
                       f"WHERE {primary_field} <= {update_row_ids}")
            case "test_table_2":
                sql = (f"UPDATE {table_name} SET "
                       f"price = (price + 10.0) "
                       f"WHERE {primary_field} <= {update_row_ids}")
            case _:
                raise ValueError(f"test_update: Unknown Table Name {table_name}")

        rows_affected = sync.sql.execute(cur=cur_source, sql=sql, params=())
        assert rows_affected == nbr_update_row_ids

    results = sync.update(tables)
    # [{'name': 'test_table_1', 'nbr_rows': 10, 'task_id': 1}]
    for result in results:
        assert result["nbr_rows"] == config.db_update_limit

    for table in tables:
        table_name = table["name"]
        if "primary_field" in table:
            primary_field = table["primary_field"]
        else:
            primary_field = config.db_primary_field

        updated_rows = sync.sql.select_one_row(cur=cur_target,
                                               sql=f"SELECT COUNT({primary_field}) AS qty FROM {table_name} "
                                                   f"WHERE name LIKE '%test update%'",
                                               params=()
                                               )
        assert updated_rows["qty"] == config.db_update_limit

    conn_source.close()
    conn_target.close()


def test_delete():
    setup_tests = SetupTests()
    qty_test_rows = 100
    config, log, sql = setup_tests.get_setup()
    config.db_delete_limit_select = 10
    config.db_delete_limit = 10
    sync = Sync(config, log, sql)

    conn_source, cur_source = sync.sql.connect_to_source()
    conn_target, cur_target = sync.sql.connect_to_target()

    table_name = "test_table_1"
    tables = [
        {"name": "test_table_1", "modified_field": "modified"},
    ]

    # delete rows
    source_ids_to_delete = [4, 7, 10]
    nbr_source_ids_to_delete = len(source_ids_to_delete)
    param_style = sql.get_param_style("position")
    sql_ids_params = ','.join([param_style] * nbr_source_ids_to_delete)
    for table in tables:
        table_name = table["name"]
        if "primary_field" in table:
            primary_field = table["primary_field"]
        else:
            primary_field = config.db_primary_field

        nbr_rows_deleted = sync.sql.execute(cur=cur_source,
                                            sql=f"DELETE FROM {table_name} "
                                                f"WHERE {primary_field} IN ({sql_ids_params})",
                                            params=source_ids_to_delete
                                            )
        assert nbr_rows_deleted == nbr_source_ids_to_delete

    results = sync.delete(tables)
    # [{'name': 'test_table_1', 'nbr_rows': 3, 'task_id': 1}]
    for result in results:
        assert result["nbr_rows"] == nbr_source_ids_to_delete

    for table in tables:
        table_name = table["name"]
        if "primary_field" in table:
            primary_field = table["primary_field"]
        else:
            primary_field = config.db_primary_field

        ids_deleted_row = sync.sql.select_one_row(cur=cur_target,
                                                  sql=f"SELECT COUNT({primary_field}) AS qty FROM {table_name} "
                                                      f"WHERE {primary_field} IN ({sql_ids_params})",
                                                  params=source_ids_to_delete
                                                  )
        assert ids_deleted_row["qty"] == 0

        ids_deleted_row = sync.sql.select_one_row(cur=cur_target,
                                                  sql=f"SELECT COUNT({primary_field}) AS qty FROM {table_name} "
                                                      f"WHERE {primary_field} NOT IN ({sql_ids_params})",
                                                  params=source_ids_to_delete
                                                  )
        assert ids_deleted_row["qty"] > config.db_delete_limit

    conn_source.close()
    conn_target.close()


def test_done():
    assert 1 == 1
