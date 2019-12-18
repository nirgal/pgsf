===========================================
Salesforce replication tools for PostgreSQL
===========================================

Requirements
============

This tools require
python3

You also need python packages ``simple_salesforce`` and ``salesforce_bulk`` available from
https://github.com/simple-salesforce/simple-salesforce.git
https://github.com/heroku/salesforce-bulk.git

Setup
=====

Copy config.py.example to config.py and customize it. This will gave you access.

You need to set up each table one by one. For example::

   ./tabledesc.py Contact

will create a file ``mapping/Contact.csv`` with the list of fields you'll want to synchronize. The default is all fields but formulas and compound fields.
