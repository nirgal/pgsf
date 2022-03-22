-- Schema where the Salesforce data will be replicated
create schema salesforce;

create type salesforce.jobstatus as enum ('ready', 'running', 'error');

create table salesforce.__sync (
	tablename varchar(70) primary key,  -- see EntityDefinition.QualifiedApiName
	syncuntil timestamp,  -- UTC
	refresh_minutes int default 10,
	last_refresh timestamp,
	status salesforce.jobstatus not null default 'ready'
);
