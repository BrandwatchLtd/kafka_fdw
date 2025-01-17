FROM ubuntu

RUN apt-get update
RUN apt-get install --assume-yes \
    wget                         \
    git

RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" \
    > /etc/apt/sources.list.d/postgres.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    | apt-key add -
RUN apt-get update
RUN apt-get install --assume-yes \
    postgresql-9.4               \
    postgresql-server-dev-9.4    \
    libcurl4-openssl-dev         \
    build-essential              \
    librdkafka1                  \
    librdkafka-dev
RUN echo "host all all 0.0.0.0/0 trust" \
    >> /etc/postgresql/9.4/main/pg_hba.conf

COPY . /project
WORKDIR /project
RUN make && make install

USER postgres

ENV PATH /usr/lib/postgresql/9.4/bin:$PATH
ENV PGDATA /var/lib/postgresql/data

EXPOSE 5432

CMD [                                                             \
    "postgres",                                                   \
        "--config_file=/etc/postgresql/9.4/main/postgresql.conf", \
        "--stats_temp_directory=/tmp",                            \
        "--listen_addresses=0.0.0.0"                              \
]
