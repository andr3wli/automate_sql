-- test if sql variables work, it does not
define a_code = 9;

select * 
from  airline_data.flight_performance
where airline_code = &a_code;