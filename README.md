

## Overview
db-sync-pie syncs databases from an active Source database to a staging Target database using multiple threads and database connections.
Sync is one way, uni-directional, Source to Target.
Target data will be overwritten with changes from Source data.

_Use at your own risk._  

To synchronize data between databases, you should consider and use alternative methods, such as:  
* Replication
* Export/Import aka Dump/Load
* Snapshots
* Copy Binary Data (rsync, sftp)
* Copy Hard Drives (Sneaker Net)
* Any other viable option

If the above options are not available due to cost, risk, availability, then db-sync-pie _might_ help you.

----
## db-sync-pie
Requirements: 
* python-3.13 (_might_ work with earlier versions)
* pipenv install 
* In both Source and Target databases, schema (table definitions) must already be in place and the same.
* Database user with SELECT, UPDATE, DELETE access.
* Databases supported:
  * sqlite3 (sqlite3; tests)
  * mariadb (mysqldb; not tested)
  * mysql (mysqldb; not tested)
  * postgres (psycopg2; not tested) 
    * create and submit a PR to add support!

### Functionality
_Inserts_ - 
In batches, finds new data by comparing MAX(primary key id) in Source tables to Target tables, inserting new rows by ids.  
_Updates_ - In batches, from Source tables, finds modified data using a pre-existing modified/timestamp field and a specified 
lookback time period, and updates Target tables.  
_Deletes_ - In batches, scans all primary key ids in Source, and compares to Target ids, deleting any ids still found in Target but not 
in Source.

_Use at your own risk._  

For a better understanding of functionality:  
* Read tests in tests/ 
* Read source in sync/
* Review logs in logs/ _(available after running)_
---
## Setup  
1. copy .env.example to .env, read .env comments and update values.  
2. copy sync_tables_[action].json.example to 3 separate files  
   add your tables and any required config
      * sync_tables_insert.json
      * sync_tables_update.json
      * sync_tables_delete.json  


sync_tables_[action].json:
example:
```
[
    {"name": "table1", "primary_field": "id", "modified_field": "timestamp"},
    {"name": "table2", "primary_field": "code", "modified_field": "modified"},
    {"name": "table3", "primary_field": "key"},
    {"name": "table4", "modified_field": "modified_at"},
    {"name": "table5"}
]
```
definition:
* **name**: _required_: table name
* **primary_field**: _optional_: primary, autoincrement, sequence field; often 'id', 'code', or 'key'
  * instead of this key, you can use the .env `DB_PRIMARY_FIELD` for all tables (example table4, table5 above)
* **modified_field**: _optional_: datetime or timestamp field which indicates the datetime the row was last modified; often 
 'modified', 'stamp', 'timestamp', 'changed', 'modified_at', etc
  * instead of this key, you can use the .env `DB_UPDATE_MODIFIED_FIELD` for all tables (example table3, table5 above)
---
## Usage
Run once:
> pipenv install  
> pipenv run python --version

Run:
```
verify .env

verify sync_tables_[action].json files

> pipenv run main.py --sync=[action=insert|update|delete|full] --dryrun
    # --dryrun = run selects, but not inserts, updates, or deletes eg test

    # insert new rows
    > pipenv run main.py --sync=insert --dryrun
    
    # update changed rows
    > pipenv run main.py --sync=update --dryrun
    
    # delete deleted rows 
    > pipenv run main.py --sync=delete --dryrun
    
    # insert, update, delete rows
    > pipenv run main.py --sync=full --dryrun
```
Utility:
```
# facilitates creation of sync_tables_[action].json files
# creates sync_tables_show_tables.json
> pipenv run main.py --select=show_tables
```

---
## Tests  
Run using [pytest](https://docs.pytest.org/en/stable/)
>  pipenv run pytest -c tests/pytest.ini  -v

---
## Extra Info
### Get list of tables for each sync_tables_[action].json 
#### mysql/mariadb:
> SHOW TABLES;  
> 
> SELECT TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '[db name]';
#### sqlite3:
> .tables  
> 
> SELECT name FROM [sqlite3 db name].sqlite_master WHERE type='table';
> 
> SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';
#### postgres:
> \dt  
> 
> SELECT table_name AS name FROM pg_catalog.pg_tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema');
#### db-sync-pie:  
```
# creates sync_tables_show_tables.json
> pipenv run main.py --select=show_tables
```

----
### Example Database triggers:
Auto update a datetime/timestamp field which indicates the datetime the row was last modified.

#### mysql/mariadb:
```
`ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

`modified` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),

`modified_at` timestamp NOT NULL DEFAULT now() ON UPDATE now(),
```

#### sqlite3:
```
DROP TRIGGER IF EXISTS "test_table_1_insert_modified";
CREATE TRIGGER test_table_1_insert_modified
    AFTER INSERT
    ON test_table_1
BEGIN
    /* DELIMITER $$ */
    UPDATE test_table_1
    SET modified = DATETIME('now')
    WHERE ROWID = NEW.ROWID
      AND (NEW.modified IS NULL OR NEW.modified != DATETIME('now')) $$
END;

DROP TRIGGER IF EXISTS "test_table_1_update_modified";
CREATE TRIGGER test_table_1_update_modified
    BEFORE UPDATE
    ON test_table_1
BEGIN
    /* DELIMITER $$ */
    UPDATE test_table_1
    SET modified = DATETIME('now')
    WHERE ROWID = NEW.ROWID
      AND (NEW.modified IS NULL OR NEW.modified == OLD.modified) $$
END;
```
#### postgres:
```
# not tested
CREATE OR REPLACE FUNCTION update_modified_column()   
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified = now();
    RETURN NEW;   
END;
$$ language 'plpgsql';
```

---

---
