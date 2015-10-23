drop extension kafka_fdw cascade;
create extension kafka_fdw;
create server kafka_openstack_alexey_1 foreign data wrapper kafka_fdw options (host '10.2.134.65', port '9092');
create foreign table kafka_openstack_alexey_1_elephants(kafka_offset bigint, kafka_value text) server kafka_openstack_alexey_1 options (topic 'test', batch_size '30000', offset '0');
select * from kafka_openstack_alexey_1_elephants limit 55;
select * from kafka_openstack_alexey_1_elephants limit 55;
explain select * from kafka_openstack_alexey_1_elephants limit 55;
explain analyze select * from kafka_openstack_alexey_1_elephants limit 55;

\des+ kafka_openstack_alexey_1
