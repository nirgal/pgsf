-- Schema where the Salesforce data will be replicated
CREATE SCHEMA salesforce;

-- Schema with synchronisation information (for internal use)
CREATE SCHEMA sync;
CREATE TABLE sync.status (
	id SERIAL PRIMARY KEY,
	tablename VARCHAR(128) UNIQUE,
	syncuntil TIMESTAMP WITHOUT TIME ZONE  -- UTC
);
