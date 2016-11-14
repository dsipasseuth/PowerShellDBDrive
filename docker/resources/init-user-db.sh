#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER docker WITH PASSWORD 'docker';
    CREATE DATABASE dockerdb;
    GRANT ALL PRIVILEGES ON DATABASE dockerdb TO docker;
    \connect dockerdb;
	CREATE SCHEMA dockerschema AUTHORIZATION docker;
	GRANT ALL PRIVILEGES ON SCHEMA dockerschema TO docker;

	CREATE TABLE dockerschema.films (
	    code        char(5) CONSTRAINT firstkey PRIMARY KEY,
	    title       varchar(40) NOT NULL,
	    did         integer NOT NULL,
	    date_prod   date,
	    kind        varchar(10)
	);
	GRANT ALL PRIVILEGES ON TABLE dockerschema.films TO docker;

EOSQL