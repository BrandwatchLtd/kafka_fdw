drop extension kafka_fdw cascade;
create extension kafka_fdw;
create server kafka_openstack_alexey_1 foreign data wrapper kafka_fdw options (host '10.2.134.65', port '9092');
create foreign table kafka_openstack_alexey_1_elephants(kafka_offset bigint, data text) server kafka_openstack_alexey_1 options (topic 'elephants');
select * from kafka_openstack_alexey_1_elephants;
explain select * from kafka_openstack_alexey_1_elephants;
explain analyze select * from kafka_openstack_alexey_1_elephants;
