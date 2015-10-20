# Kafka Foreign Data Wrapper for PostgreSQL
# Kafka Foreign Data Wrapper for PostgreSQL

This provides a foreign data wrapper for Kafka which allows it to be treated as
a table. Each select from the table will read new rows from the Kafka Queue.

If any limit is applied then only that many messages are returned. This limit
is applied before any where clauses.

## Use

You can build the extension using docker-compose. This will create a postgres
container which has access to the wrapper. This can be useful for evaluating
the wrapper.

You do this as follows:

    docker-compose build
    docker-compose up
    docker exec -ti kafka_fdw psql template1

You can also build this normally:

    make
    sudo make install
    psql template1

Once this has been built you can then load the extension with the following
psql command:

    CREATE EXTENSION kafka_fdw;

Examples of use can be found in sql/kafka_fdw.sql

## Table Schema

