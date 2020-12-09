# pg_report
This python program does a general health check on a specific PostgreSQL cluster.

(c) 2016-2020 SQLEXEC LLC
<br/>
GNU V3 and MIT licenses are conveyed accordingly.
<br/>
Bugs can be reported @ michaeldba@sqlexec.com

## Overview
It checks for load, bloat, long queries, blocked queries

## Requirements
1. python 2.6 or 2.7
2. python packages: python-psutil, psycopg2
3. psql client 
3. psutil for windows only: https://pypi.python.org/pypi?:action=display&name=psutil#downloads

You can see a sample report here:
@ https://rawgit.com/MichaelDBA/pg_maint/master/SampleReport.html

## Inputs
All fields are optional except database and action. The verbose flag is only intended as a debugging feature.

`-h <hostname or IP address> `
<br/>
`-d <database> `
<br/>
`-n <schema>`
<br/>
`-p <PORT>`
<br/>
`-U <db user>`
<br/>
`-m [html format flag] `
<br/>
`-r [dry run flag] `
<br/>
`-v [verbose output flag, mostly used for debugging]`
<br/>
## Examples
Run report on entire test database and output to html format for web browser viewing:

`./pg_maint.py -d test -a report --html`


## Assumptions
1. db user defaults to postgres if not provided as parameter.
2. db port defaults to 5432 if not provided as parameter.
3. Password must be in local .pgpass file or client authentication changed to trust or peer
4. psql must be in the user's path

## Report logic
1.  Cache Hit Ratio
2.  Connections
3.  Idle in Transactions
4.  Long Running Queries
5.  Lock Waits
6.  Archiving Status
7.  Database conflicts, deadlocks, and temp_files.
8.  Checkpoint Frequency
9.  Checkpoint, Background, and Backend Writers
10. Identify orphaned large objects.
11.  Bloated tables/indexes are identified where at least 20% of the table/index is bloated or the wasted bytes is > 10 GB.
12. Unused indexes
13. Vacuum Freeze Candidates
14. Analyze/Vacuum Analyze candidates
15. PG memory configuration settings
16. Linux Kernel Memory Capacity

