drop extension kafka_fdw cascade;
create extension kafka_fdw;
create server kafka_openstack_alexey_1 foreign data wrapper kafka_fdw options (host '10.2.134.65', port '9092');
create foreign table kafka_openstack_alexey_1_valid(kafka_offset bigint, kafka_value text) server kafka_openstack_alexey_1 options (topic 'test', batch_size '30000', offset '26528300');
select * from kafka_openstack_alexey_1_valid limit 5;
select * from kafka_openstack_alexey_1_valid limit 5;
explain select * from kafka_openstack_alexey_1_valid limit 5;
explain analyze select * from kafka_openstack_alexey_1_valid limit 100000;

explain analyze select * from kafka_openstack_alexey_1_valid;
explain select * from (select 1 from generate_series(1, 10)) _ cross join kafka_openstack_alexey_1_valid b;
select * from (select 1 from generate_series(1, 10)) _ cross join kafka_openstack_alexey_1_valid b;

create foreign table kafka_openstack_alexey_1_invalid1(kafka_offset bigint, kafka_value text, foo_bar int) server kafka_openstack_alexey_1 options (topic 'test', batch_size '30000', offset '26528300');
select * from kafka_openstack_alexey_1_invalid1;

create foreign table kafka_openstack_alexey_1_invalid2(kafka_offset bigint, kafka_value json) server kafka_openstack_alexey_1 options (topic 'test', batch_size '30000', offset '26528300');
select * from kafka_openstack_alexey_1_invalid2;



