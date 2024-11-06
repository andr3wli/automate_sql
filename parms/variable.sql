-- does variable work in this?
define animal = "cat";

select distinct(pay) from andrew.test_table
    where animal = &cat and start_date > date'2023-03-31';