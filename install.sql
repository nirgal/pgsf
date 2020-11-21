-- Schema where the Salesforce data will be replicated
create schema salesforce;

create table salesforce.__sync (
	tablename varchar(70) primary key,  -- see EntityDefinition.QualifiedApiName
	syncuntil timestamp  -- UTC
);
