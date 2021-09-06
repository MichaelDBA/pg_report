# pg_report
This python program does a general health check on a specific PostgreSQL cluster.

(c) 2016-2021 SQLEXEC LLC
<br/>
GNU V3 and MIT licenses are conveyed accordingly.
<br/>
Bugs can be reported @ michaeldba@sqlexec.com

## Overview
It checks for a bunch of database health metrics including load, bloat, long queries, blocked queries, old versions, etc.

## Requirements
1. python 2.6 or higher
2. python packages: python-psutil, psycopg2
3. psql client 
4. psutil for windows only: https://pypi.python.org/pypi?:action=display&name=psutil#downloads
5. postgresql contrib package is necessary for vacuumlo functionality

You can see a sample report here:
@ http://htmlpreview.github.io/?https://github.com/MichaelDBA/pg_report/blob/gh-pages/pg_report_example.html

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

`./pg_report.py -d test --html`


## Assumptions
1. db user defaults to postgres if not provided as parameter.
2. db port defaults to 5432 if not provided as parameter.
3. Password must be in local .pgpass file or client authentication changed to trust or peer
4. psql must be in the user's path
5. No .psqlrc file is used.

## Report logic
1.  PG Major/Minor check
2.  Cache Hit Ratio
3.  Shared Preload Libraries
4.  Connections
5.  Idle in Transactions
6.  Long Running Queries
7.  Lock Waits
8.  Archiving Status
9.  Database conflicts, deadlocks, and temp_files.
10.  Checkpoint Frequency
11.  Configuration settings.
12.  Checkpoint, Background, and Backend Writers
13. Identify orphaned large objects.
14.  Bloated tables/indexes are identified where at least 20% of the table/index is bloated or the wasted bytes is > 10 GB.
15. Unused indexes
16. Vacuum Freeze Candidates
17. Analyze/Vacuum Analyze candidates
18. PG memory configuration settings
19. Linux Kernel Memory Capacity

