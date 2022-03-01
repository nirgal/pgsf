===========================================
Salesforce replication tools for PostgreSQL
===========================================

Requirements
============

This tools requires
python3 python3-psutil python3-psycopg2 python3-six python3-requests python3-unicodecsv

You also need python packages ``simple_salesforce`` and ``salesforce_bulk`` available from:

- https://github.com/simple-salesforce/simple-salesforce.git
- https://github.com/heroku/salesforce-bulk.git

Setup
=====

Copy pgsf.example to ~/.pgsf and customize it with your Saleforce and Prosgres credentials.

The PostgreSQL database should already be created, with the schemas created. See ``install.sql``

Download a table
================

You need to set up each table one by one. For example::

   ./tabledesc.py Contact

will create a file ``mapping/Contact.csv`` with the list of fields you'll want to synchronize. The default is all fields but formulas and compound fields.


::

   ./query_bulk.py Contact

will create a bulk download job, and print a jobid.

::

   ./download.py <jobid>

will download all the csv from that job.

::

   ./createtable.py Contact

will create a PostgreSQL version of a SF table

::

   ./csv_to_postgres.py <jobid>

will import the job csv files into a PostgreSQL table.

::

   ./query_poll_table.py Contact

will download only updates, and will import them in the PostgreSQL table.


TODO
====

Have a single command that runs query_bulk.py + download.py + csv_to_postgres

Check how many records would be fetch by a query_poll_table, and switch to query if it's above a certain limit.

When doing a csv_to_postgres, running queru_poll_table should be killed and __sync status should switch to 'running'.
