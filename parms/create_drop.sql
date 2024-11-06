CREATE TABLE andrew.my_table_test AS
select distinct(pay) from andrew.test_table
    where animal = "dog" and start_date > date'2023-03-31';

CREATE TABLE andrew.my_table_test_2 AS
select distinct(pay) from andrew.test_table
    where animal = "dog" and start_date > date'2023-03-31';


-- drop table andrew.my_table_test;
-- drop table andrew.my_table_test_2;