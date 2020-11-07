-- Schema where the Salesforce data will be replicated
create schema salesforce;

-- Schema with synchronisation information (for internal use)
create schema sync;
create table sync.status (
	tablename varchar(70) primary key,  -- see EntityDefinition.QualifiedApiName
	syncuntil timestamp  -- UTC
);
