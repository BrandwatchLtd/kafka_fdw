CREATE EXTENSION kafka_fdw;

CREATE SERVER s_kafka
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS ();

CREATE FOREIGN TABLE t_kafka
(
    i_offset bigint,
    t_value text
)
SERVER s_kafka
OPTIONS
(
    topic 'test',
    batch_size: '30000'
);

SELECT
    i_offset,
    t_value
FROM
    t_kafka
LIMIT 1;
