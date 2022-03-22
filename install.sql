-- Schema where the Salesforce data will be replicated
create schema salesforce;

create type salesforce.jobstatus as enum ('ready', 'running', 'error');

-- see comments bellow
create table salesforce.__sync (
	tablename varchar(70) primary key,
	syncuntil timestamp,
	refresh_minutes int default 10,
	last_refresh timestamp,
	status salesforce.jobstatus not null default 'ready'
);
comment on column salesforce.__sync.tablename is 'From SF EntityDefinition.QualifiedApiName';
comment on column salesforce.__sync.syncuntil is 'UTC';
comment on column salesforce.__sync.last_refresh is 'Local time';
