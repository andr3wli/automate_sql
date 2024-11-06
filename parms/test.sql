-- test will comments mess this up?
/* what about this comment?*/
select distinct(pay) from andrew.test_table
    where animal = "dog" and start_date > date'2023-03-31';