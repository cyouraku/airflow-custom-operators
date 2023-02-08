DECLARE
tbl_count number;
sql_stmt long;

BEGIN
    SELECT COUNT(*) INTO tbl_count 
    FROM dba_tables
    WHERE owner = 'sandbox'
    AND table_name = 'test_oracle_to_mysql';

    IF(tbl_count <> 0)
        THEN
        sql_stmt:='DROP TABLE test_oracle_to_mysql';
        EXECUTE IMMEDIATE sql_stmt;
    END IF;
END;