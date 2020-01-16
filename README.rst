===========================================
Salesforce replication tools for PostgreSQL
===========================================

Requirements
============

This tools requires
python3 python3-psycopg2

You also need python packages ``simple_salesforce`` and ``salesforce_bulk`` available from
https://github.com/simple-salesforce/simple-salesforce.git
https://github.com/heroku/salesforce-bulk.git

Setup
=====

Copy config.py.example to config.py and customize it. This will gave you access.

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


TODO
====

Create the indexes automatically.
