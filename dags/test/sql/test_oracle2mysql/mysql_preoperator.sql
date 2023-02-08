use sandbox;

set sql_notes=0;

drop table if exists `test_oracle_to_mysql`;

create table if not exists `test_oracle_to_mysql` (
    `col1` int(11) unsigned not null,
    `col2` varchar(20),
    `col3` varchar(20),
    `col4` varchar(20),
    `col5` varchar(20),
    `col6` varchar(20),
    `col7` varchar(20),
    `col8` varchar(20),
    `col9` varchar(20),
    primary key(`col1`)
);