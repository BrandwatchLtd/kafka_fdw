\timing off

DROP DATABASE IF EXISTS kafka;
CREATE DATABASE kafka;
\c kafka

CREATE EXTENSION kafka_fdw;

CREATE SERVER s_kafka
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (host 'kafka');

CREATE FOREIGN TABLE t_kafka
(
    i_offset bigint,
    t_value text
)
SERVER s_kafka
OPTIONS
(
    topic 'test',
    batch_size '30000',
    offset '-2'
);

SELECT
    i_offset,
    t_value
FROM
    t_kafka
LIMIT 1;

SELECT
    i_offset,
    t_value
FROM
    t_kafka
OFFSET 100
LIMIT 100;

SELECT
    t_value
FROM
    t_kafka
OFFSET 250000
LIMIT 5;

SELECT
    i_offset
FROM
    t_kafka
LIMIT 5;

EXPLAIN SELECT
    i_offset,
    t_value
FROM
    t_kafka
LIMIT 5;
