-- Schema where the Salesforce data will be replicated
CREATE SCHEMA salesforce;

-- Schema with synchronisation information (for internal use)
CREATE SCHEMA sync;
CREATE TABLE sync.status (
	tablename VARCHAR(128) PRIMARY KEY,
	syncuntil TIMESTAMP  -- UTC
);
