BEGIN 
    EXECUTE IMMEDIATE 'DROP TABLE test_oracle_to_mysql';
    EXCEPTION WHEN OTHERS THEN NULL;

    CREATE TABLE test_oracle_to_mysq (
        col1 number(10) check (col1 > 0) not null,
        col2 varchar(20),
        col3 varchar(20),
        col4 varchar(20),
        col5 varchar(20),
        col6 varchar(20),
        col7 varchar(20),
        col8 varchar(20),
        col9 varchar(20),
        primary key(col1)
    );
END;