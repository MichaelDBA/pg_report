# pg_report
This python program reports problems within a PostgreSQL Instance based on input action specified.

(c) 2016-2020 SQLEXEC LLC
<br/>
GNU V3 and MIT licenses are conveyed accordingly.
<br/>
Bugs can be reported @ michaeldba@sqlexec.com

## Overview
It checks for load, bloat, long queries, blocked queries

## Requirements
1. python 2.7 or higher
2. python packages: python-psutil, psycopg2

## examples
`/apps/opt/postgres/sc/pg_report.py -H $HOST -d $DB -p 5432 -u dbadmin --action load --cpus 64 --cpumaxpct 80 -s 'LOCUS PROD WRITER' --notify  >> /postgres/home/log/pg_report.log 2>&1`
<br/><br/>
`/apps/opt/postgres/sc/pg_report.py -H $HOST -d $DB -p 5432 -u dbadmin --action longquery --maxmin 10 -s 'LOCUS PROD WRITER' --notify  >> /postgres/home/log/pg_report.log 2>&1`
<br/><br/>
`/apps/opt/postgres/sc/pg_report.py -H $HOST -d $DB -p 5432 -u dbadmin --action blockedqueries -s 'LOCUS PROD WRITER'  --notify >> /postgres/home/log/pg_report.log 2>&1`
<br/><br/>
`/apps/opt/postgres/sc/pg_report.py -H $HOST -d $DB -p 5432 -u dbadmin --action uptime -s 'LOCUS PROD WRITER' --notify  >> /postgres/home/log/pg_report.log 2>&1`
<br/><br/>
`/apps/opt/postgres/sc/pg_report.py -H $HOST -d $DB -p 5432 -u dbadmin --action bloat --bloatpct 50 -s 'LOCUS PROD WRITER' --notify  >> /postgres/home/log/pg_report.log 2>&1`
<br/><br/>

