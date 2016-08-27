/* 
(71,,262, -- id,_,bike_id
2015-02-01 00:48:00+00,2015-02-01 00:52:00+00,  -- starttime,endtime
449,W 52 St & 9 Ave,40.76461837,-73.98789473, --startstation_id, address,lat and lon
457,Broadway & W 58 St,40.76695317,-73.98169333, --endstation_id, address,lat and lon
19687,Subscriber,1961,1)  -- some_no., type, year,somenumber2
*/
SET default_parallel 2;
A = LOAD '/data/share/bdm/citibike.csv' USING PigStorage(',') AS 
(id:int,empty:chararray,bike_id:int,
start_time:chararray,end_time:chararray,
start_station_id:int,start_address:chararray,start_lat:float,start_lon:float,
end_station_id:int, end_address:chararray, end_lat:float, end_lon:float,
some_number:int, type:chararray,birth_year:int,some_number2:int);


grouped_data = group A by birth_year;
DUMP grouped_data;
