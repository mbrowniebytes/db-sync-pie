-- schema
DROP TABLE IF EXISTS "test_table_1";
CREATE TABLE IF NOT EXISTS "test_table_1" (
    "id" INTEGER NOT NULL,
    "name" VARCHAR ( 64 ) NOT NULL DEFAULT '',
    "address" VARCHAR ( 128 ) NOT NULL DEFAULT '',
    "modified" DATETIME NULL DEFAULT NULL,
	PRIMARY KEY ( "id" )
);

-- triggers
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
--------------------------------------------------------------------------------

-- schema
DROP TABLE IF EXISTS "test_table_2";
CREATE TABLE IF NOT EXISTS "test_table_2" (
    "id" INTEGER NOT NULL,
    "item" VARCHAR ( 64 ) NOT NULL DEFAULT '',
    "price" DECIMAL ( 10, 4 ) NOT NULL DEFAULT 0.0000,
    "modified" DATETIME NULL DEFAULT NULL,
	PRIMARY KEY ( "id" )
);

-- triggers
DROP TRIGGER IF EXISTS "test_table_2_insert_modified";
CREATE TRIGGER test_table_2_insert_modified
    AFTER INSERT
    ON test_table_2
BEGIN
    /* DELIMITER $$ */
    UPDATE test_table_2
    SET modified = DATETIME('now')
    WHERE ROWID = NEW.ROWID
      AND (NEW.modified IS NULL OR NEW.modified != DATETIME('now')) $$
END;

DROP TRIGGER IF EXISTS "test_table_2_update_modified";
CREATE TRIGGER test_table_2_update_modified
    BEFORE UPDATE
    ON test_table_2
BEGIN
    /* DELIMITER $$ */
    UPDATE test_table_2
    SET modified = DATETIME('now')
    WHERE ROWID = NEW.ROWID
      AND (NEW.modified IS NULL OR NEW.modified == OLD.modified) $$
END;
--------------------------------------------------------------------------------
