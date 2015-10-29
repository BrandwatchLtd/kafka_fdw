\timing off

DROP DATABASE IF EXISTS kafka;
CREATE DATABASE kafka;
\c kafka

CREATE EXTENSION kafka_fdw;

CREATE SERVER s_kafka
FOREIGN DATA WRAPPER kafka_fdw;

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
