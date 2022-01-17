-- Schema where the Salesforce data will be replicated
create schema salesforce;

create table salesforce.__sync (
	tablename varchar(70) primary key,  -- see EntityDefinition.QualifiedApiName
	syncuntil timestamp  -- UTC
);

alter table salesforce.__sync add column refresh_minutes int default 10;
alter table salesforce.__sync add column last_refresh timestamp;
