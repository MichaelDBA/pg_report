#!/usr/bin/env python
###############################################################################
### COPYRIGHT NOTICE FOLLOWS.  DO NOT REMOVE
###############################################################################
### Copyright (c) 2019 - 2020, SQLEXEC LLC
###
### This program is bound by the following licenses:
###    GNU GENERAL PUBLIC LICENSE Version 3, 29 June 2007
###    MIT licensing privileges also conveyed on top of GNU V3.
###
### Permission to use, copy, modify, and distribute this software and its
### documentation for any purpose, without fee, and without a written agreement
### is hereby granted, provided that the above copyright notice and this paragraph
### and the following two paragraphs appear in all copies.
###
### IN NO EVENT SHALL SQLEXEC LLC BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
### SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
### ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
### SQLEXEC LLC HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
###
### SQLEXEC LLC SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
### LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
### PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS,
### AND SQLEXEC LLC HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
### ENHANCEMENTS, OR MODIFICATIONS.
###
#######################################G########################################
# pg_report.py
#
# author: Michael Vitale, michaeldba@sqlexec.com
#
# Description: This program reports problems within a PostgreSQL Instance based on input action specified.
#
# Date Created : March 19, 2019   Original Coding (v 1.0)
#                                 Tested with CentOS 7, PostgreSQL v9.6 and v12, Python 2.7.5
# Date Modified:
# March 26, 2020.  V 2.0          Refactoring
# April 03, 2020.  V 2.1          Added logic for blocked SQL and uptime
# April 17, 2020.  V 2.2          Bug Fixes, cleanup, reformatting
#
# Notes:
#  1. For load determination, it is assumed as a general rule that 100% load is when 2 * #CPUs = #active connections
#
# call example with all parameters:
# ./pg_report.py -H localhost -d GWYVLATI -p 5432 -u dbadmin --action load --cpus 64 --cpumaxpct 100 -s 'LOCUS PROD' --notify
#  load check:
# ./pg_report.py -H nts-gwyv-postgsql-rds-a5-dbcluster-1okgij4zg2gqi.cluster-cl9vgbtolm5s.us-east-1.rds.amazonaws.com -d GWYVLATI -p 5432 -u dbadmin --action load --cpus 64 --cpumaxpct 100 -s 'LOCUS PROD WRITER' --notify
#  longquery check:
# ./pg_report.py -H nts-gwyv-postgsql-rds-a5-dbcluster-1okgij4zg2gqi.cluster-cl9vgbtolm5s.us-east-1.rds.amazonaws.com -d GWYVLATI -p 5432 -u dbadmin --action longqueries --maxmin 10 -s 'LOCUS PROD WRITER' --notify
#  blockedquery check:
# ./pg_report.py -H nts-gwyv-postgsql-rds-a5-dbcluster-1okgij4zg2gqi.cluster-cl9vgbtolm5s.us-east-1.rds.amazonaws.com -d GWYVLATI -p 5432 -u dbadmin --action blockedqueries -s 'LOCUS PROD WRITER' --notify
#
# crontab example that runs every 5 minutes
# SHELL=/bin/sh
# PATH=<your binary paths as postgres user>
#
# */5  * * * * /home/postgres/sc/pg_report.py -H nts-gwyv-postgsql-rds-a5-dbcluster-1okgij4zg2gqi.cluster-cl9vgbtolm5s.us-east-1.rds.amazonaws.com -d GWYVLATI -p 5432 -u dbadmin --action load --cpus 64 -s 'LOCUS PROD'
# --cpumaxpct 80 -s 'LOCUS PROD' --notify  >> /home/postgres/sc/pg_report.log 2>&1
#
# */15 * * * * /home/postgres/sc/pg_report.py -H nts-gwyv-postgsql-rds-a5-dbcluster-1okgij4zg2gqi.cluster-cl9vgbtolm5s.us-east-1.rds.amazonaws.com -d GWYVLATI -p 5432 -u dbadmin --action longquery --maxmin 10 -s 'LOCUS PROD'
# -s 'LOCUS PROD' --notify  >> /home/postgres/sc/pg_report.log 2>&1
#
# */2  * * * * /home/postgres/sc/pg_report.py -H nts-gwyv-postgsql-rds-a5-dbcluster-1okgij4zg2gqi.cluster-cl9vgbtolm5s.us-east-1.rds.amazonaws.com -d GWYVLATI -p 5432 -u dbadmin --action blockedqueries -s 'LOCUS PROD WRITER' 
# --notify >> /home/postgres/sc/pg_report.log 2>&1
#
# */20 * * * * /home/postgres/sc/pg_report.py -H nts-gwyv-postgsql-rds-a5-dbcluster-1okgij4zg2gqi.cluster-cl9vgbtolm5s.us-east-1.rds.amazonaws.com -d GWYVLATI -p 5432 -u dbadmin --action uptime -s 'LOCUS PROD' -s 'LOCUS PROD'  # --notify  >> /home/postgres/sc/pg_report.log 2>&1
#
# 00 01 * * * /home/postgres/sc/pg_report.py -H nts-gwyv-postgsql-rds-a5-dbcluster-1okgij4zg2gqi.cluster-cl9vgbtolm5s.us-east-1.rds.amazonaws.com -d GWYVLATI -p 5432 -u dbadmin --action bloat --bloatpct 70 -s 'LOCUS PROD' -s
# 'LOCUS PROD'  # --notify  >> /home/postgres/sc/pg_report.log 2>&1
###############################################################################
import sys, os, threading, argparse, time, datetime, signal
from optparse import OptionParser
import psycopg2

version = 'pg_report 2.2  April 17, 2020'
OK = 0
BAD = -1

fmtdate  = '                   '
fmtrows  = '%11d'
fmtbytes = '%13d'


'''
def get_process_cnt():
    cmd = "ps -ef | grep 'VACUUM VERBOSE\|ANALYZE VERBOSE' | grep -v grep | wc -l"
    result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
    result = int(result.decode('ascii'))
    return result

def check_load_linux():
    cmd = "uptime | sed 's/.*load average: //' | awk -F\, '{print $1}'"
    result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
    min1  = str(result.decode())
    loadn = int(float(min1))
    cmd = "cat /proc/cpuinfo | grep processor | wc -l"
    result = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
    cpus  = str(result.decode())
    cpusn = int(float(cpus))
    load = (loadn / cpusn) * 100
    if load < load_threshold:
        return False
    else:
        printit ("High Load: loadn=%d cpusn=%d load=%d" % (loadn, cpusn, load))
        return True
'''

def signal_handler(signal, frame):
     printit('User-interrupted!')
     sys.exit(1)

def printit(text):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    txt = now + ' ' + text
    print (txt)
    # don't use v3 way to flush output since we want this to work for v2 and 3
    #print (txt, flush=True)
    sys.stdout.flush()
    return

def execute_cmd(text):
    rc = os.system(text)
    return rc

def send_mail(to, from_, subject, body):
    # assumes nonprintables are already removed from the body, else it will send it as an attachment and not in the body of the email!
    msg = 'echo "%s" | mail -s "%s" -r %s -- %s' % (body, subject, to, from_)
    #print msg    
    rc = os.system(msg)
    if rc == 0:
        printit("email sent successfully.")
    return rc

def check_load_db(cpumaxpct):
    sql = "select count(*) as active from pg_stat_activity where state in ('active', 'idle in transaction')"
    try:
        cur.execute(sql)
    except psycopg2.Error as e:
        printit ("Load Select Error: %s" % (e))
        return 1

    rows = cur.fetchone()
    activecnt = float(rows[0])
    saturation = float(activecnt * 2)
    loadpct = round(activecnt / cpus, 2) * 100
    loadint = int(loadpct)

    if loadpct > cpumaxpct:
        body    = "%s (LOAD) ** Load %d%%: cpumaxpct=%d  active=%d  cpus=%d  loadpct=%4.2f" % (serverid, loadpct, cpumaxpct, activecnt, cpus, loadint)
        printit(body)
        if notify:
            to      = 'pgdude@noreply.com michael.vitale@verizon.com'
            from_   = 'pgdude@noreply.com'
            #subject = 'Server `hostname` : `date`  test subject'
            subject = 'Server %s : `date`  load saturation' % serverid
            rc = send_mail(to, from_, subject, body)
            if rc <> 0:
                printit("mail error")
                return 1
    else:
        printit("%s ** Acceptable Load %d%%: cpumaxpct=%d  active=%d  cpus=%d  loadpct=%4.2f" % (serverid, loadpct, cpumaxpct, activecnt, cpus, loadint))
    return 0    

        
def check_longqueries():
    '''
    select datname, pid, client_addr,usename, application_name as app, to_char(query_start, 'YYYY-MM-DD HH24:MI:SS') as query_start, cast(EXTRACT(EPOCH FROM (now() - backend_start)) as integer) as conn_secs,(case when state <> 'active' then cast(EXTRACT(EPOCH FROM (now() - state_change)) as integer) else -1 end) as idle_secs, (case when state  = 'active' then cast(EXTRACT(EPOCH FROM (now() - query_start)) as integer) else -1 end) as q_secs, state, coalesce(wait_event_type, 'N/A') as wait_event_type, coalesce(wait_event, 'N/A') as wait_event, regexp_replace(replace(regexp_replace(substring(query,1,200), E'[\\n\\r]+', ' ', 'g' ),'    ',''), '[^\x20-\x7f\x0d\x1b]', '', 'g') as query from pg_stat_activity where state = 'active' order by 9 desc;
    '''
    sql = \
      "select datname, pid, client_addr,usename, application_name as app, to_char(query_start, 'YYYY-MM-DD HH24:MI:SS') as query_start, cast(EXTRACT(EPOCH FROM (now() - backend_start)) as integer) as conn_secs, " \
      "(case when state <> 'active' then cast(EXTRACT(EPOCH FROM (now() - state_change)) as integer) else -1 end) as idle_secs, (case when state  = 'active' then cast(EXTRACT(EPOCH FROM (now() - query_start)) as integer) " \
      "else -1 end) as q_secs, state, coalesce(wait_event_type, 'N/A') as wait_event_type, coalesce(wait_event, 'N/A') as wait_event, " \
      "regexp_replace(replace(regexp_replace(substring(query,1,%d), E'[\\n\\r]+', ' ', 'g' ),'    ',''), '[^\x20-\x7f\x0d\x1b]', '', 'g') as query from pg_stat_activity where state = 'active' order by 9 desc" % (maxquerylen)
         
    try:
        cur.execute(sql)
    except psycopg2.Error as e:
        printit ("Long Queries Select Error: %s" % (e))
        return 1

    rows = cur.fetchall()
    if len(rows) == 0:
        printit ("No long queries.")

    cnt = 0
    lqcnt = 0
    body = ''
    for row in rows:
        cnt = cnt + 1
        datname         = row[0]
        pid             = row[1]
        client_addr     = row[2]
        usename         = row[3]
        app             = row[4]
        query_start     = row[5]
        conn_secs       = row[6]    
        idle_secs       = row[7]    
        q_secs          = row[8]   
        state           = row[9]           
        wait_event_type = row[10]    
        wait_event      = row[11]    
        query           = row[12]    

        if q_secs < 10:
            continue

        q_mins = q_secs / 60
        
        if q_mins > maxmin:
            lqcnt = lqcnt + 1
            body = body + "%s (LONG QUERY) ** db=%s -- pid=%s -- claddr=%s -- user=%s -- app=%s -- qstart=%s -- connsecs=%s -- idlesecs=%s -- qsecs=%s -- wait_event=%s -- we_type=%s -- \nquery=%s\n" \
                   % (serverid, datname, pid, client_addr, usename, app, query_start, conn_secs, idle_secs, q_secs, wait_event_type, wait_event, query)

    if notify and lqcnt > 0:
        to      = 'pgdude@noreply.com michael.vitale@verizon.com'
        from_   = 'pgdude@noreply.com'
        #subject = 'Server `hostname` : `date`  test subject'
        subject = 'Server %s : `date`  long query(s)=%d ' % (serverid, lqcnt)
        rc = send_mail(to, from_, subject, body)
        if rc <> 0:
            printit("mail error")
            return 1
    if lqcnt > 0:        
        printit("%s ** Long Queries Detected = %d" % (serverid, lqcnt))
        printit("%s" % body)
    else:
        printit("%s ** No long queries > %d minutes." % (serverid,maxmin))    
    return 0
    
def check_blockedqueries():
    '''
    SELECT blocked_activity.datname as blocked_db, blocked_locks.pid AS blocked_pid, blocked_activity.usename  AS blocked_user, blocked_activity.state as blocked_state, blocked_activity.wait_event as blocked_wait_event, blocked_activity.wait_event_type as blocked_wait_event_type, blocked_activity.query_start as blocked_query_start, (case when blocked_activity.state  = 'active' then cast(EXTRACT(EPOCH FROM (now() - blocked_activity.query_start)) as integer) else -1 end) as blocked_secs, blocking_activity.datname as blocking_db, blocking_locks.pid AS blocking_pid, blocking_activity.usename AS blocking_user, blocking_activity.state as blocking_state, blocking_activity.wait_event as blocking_wait_event, blocking_activity.wait_event_type as blocking_wait_event_type, blocking_activity.query_start as blocking_query_start, cast(EXTRACT(EPOCH FROM (now() - blocking_activity.query_start)) as integer) as blocking_secs, regexp_replace(replace(regexp_replace(substring(blocked_activity.query,1,9999), E'[\\n\\r]+', ' ', 'g' ),'    ',''), '[^\x20-\x7f\x0d\x1b]', '', 'g') AS blocked_statement, regexp_replace(replace(regexp_replace(substring(blocking_activity.query,1,9999), E'[\\n\\r]+', ' ', 'g' ),'    ',''), '[^\x20-\x7f\x0d\x1b]', '', 'g') AS blocking_statement FROM pg_catalog.pg_locks blocked_locks JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid AND blocking_locks.pid != blocked_locks.pid JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid WHERE NOT blocked_locks.GRANTED;
    '''
    sql = \
        "SELECT blocked_activity.datname as blocked_db, blocked_locks.pid AS blocked_pid, blocked_activity.usename  AS blocked_user, blocked_activity.state as blocked_state, " \
        "blocked_activity.wait_event as blocked_wait_event, blocked_activity.wait_event_type as blocked_wait_event_type, blocked_activity.query_start as blocked_query_start, " \
        "(case when blocked_activity.state  = 'active' then cast(EXTRACT(EPOCH FROM (now() - blocked_activity.query_start)) as integer) else -1 end) as blocked_secs, " \
        "blocking_activity.datname as blocking_db, blocking_locks.pid AS blocking_pid, blocking_activity.usename AS blocking_user, blocking_activity.state as blocking_state, " \
        "blocking_activity.wait_event as blocking_wait_event, blocking_activity.wait_event_type as blocking_wait_event_type, blocking_activity.query_start as blocking_query_start, " \
        "cast(EXTRACT(EPOCH FROM (now() - blocking_activity.query_start)) as integer) as blocking_secs, " \
        "regexp_replace(replace(regexp_replace(substring(blocked_activity.query,1,9999), E'[\\n\\r]+', ' ', 'g' ),'    ',''), '[^\x20-\x7f\x0d\x1b]', '', 'g') AS blocked_statement, " \
        "regexp_replace(replace(regexp_replace(substring(blocking_activity.query,1,9999), E'[\\n\\r]+', ' ', 'g' ),'    ',''), '[^\x20-\x7f\x0d\x1b]', '', 'g') AS blocking_statement " \
        "FROM pg_catalog.pg_locks blocked_locks JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid JOIN pg_catalog.pg_locks blocking_locks " \
        "ON blocking_locks.locktype = blocked_locks.locktype AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE AND blocking_locks.relation IS NOT DISTINCT " \
        "FROM blocked_locks.relation AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple AND " \
        "blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid AND " \
        "blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid AND blocking_locks.objsubid IS NOT DISTINCT " \
        "FROM blocked_locks.objsubid AND blocking_locks.pid != blocked_locks.pid JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid WHERE NOT blocked_locks.GRANTED"
    try:
        cur.execute(sql)
    except psycopg2.Error as e:
        printit ("Blocked Queries Select Error: %s" % (e))
        return 1

    rows = cur.fetchall()
    if len(rows) == 0:
        printit("%s ** No blocked SQL Detected." % (serverid))            
        return 0

    cnt = 0
    lqcnt = 0
    body = ''
    for row in rows:
        cnt = cnt + 1
        blocked_db              = row[0]
        blocked_pid             = row[1]
        blocked_user            = row[2] 
        blocked_state           = row[3]
        blocked_wait_event      = row[4] 
        blocked_wait_event_type = row[5]
        blocked_sql_start       = row[6] 
        blocked_secs            = int(row[7])
        blocking_db             = row[8]
        blocking_pid            = row[9] 
        blocking_user           = row[10] 
        blocking_state          = row[11]
        blocking_wait_event     = row[12] 
        blocking_wait_event_type= row[13]
        blocking_sql_start      = row[14] 
        blocking_secs           = int(row[15])
        blocked_sql             = row[16] 
        blocking_sql            = row[17]         

        if blocked_secs < 10:
            continue
       
        lqcnt = lqcnt + 1
        body = body + \
               "%s (BLOCKED  INFO) *** db=%s pid=%s user=%s state=%20s wait_e=%15s wait_et=%10s start=%s  secs=%d\n" \
               "%s (BLOCKING INFO) *** db=%s pid=%s user=%s state=%20s wait_e=%15s wait_et=%10s start=%s  secs=%d\n" \
               "blocked_sql =%s\n" \
               "blocking_sql=%s\n\n" \
               % (serverid, blocked_db, blocked_pid, blocked_user, blocked_state, blocked_wait_event, blocked_wait_event_type, blocked_sql_start, blocked_secs, serverid, blocking_db, blocking_pid, blocking_user, \
                  blocking_state, blocking_wait_event, blocking_wait_event_type, blocking_sql_start, blocking_secs, blocked_sql, blocking_sql)
               
    if notify and lqcnt > 0:
        to      = 'pgdude@noreply.com michael.vitale@verizon.com'
        from_   = 'pgdude@noreply.com'
        #subject = 'Server `hostname` : `date`  test subject'
        subject = 'Server %s : `date`  blocked query(s)=%d ' % (serverid, lqcnt)
        rc = send_mail(to, from_, subject, body)
        if rc <> 0:
            printit("mail error")
            return 1
    if lqcnt > 0:        
        printit("%s ** Blocked SQL Detected = %d" % (serverid, lqcnt))
        printit("%s" % body)
    else:
        printit("%s ** No blocked SQL Detected." % (serverid))    
    return 0    

def check_uptime():
    '''
    select pg_postmaster_start_time() as starttime, cast(EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) as integer)::integer / 60 as uptime;
    '''
    sql = "select pg_postmaster_start_time() as starttime, cast(EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) as integer)::integer / 60 as uptime"
    try:
        cur.execute(sql)
    except psycopg2.Error as e:
        printit ("Server Uptime Error: %s" % (e))
        return 1

    rows = cur.fetchone()
    if len(rows) == 0:
        printit ("Unable to get server uptime.")

    body = ''
    uptime_start    = rows[0]
    uptime_minutes  = rows[1]
    if uptime_minutes > 3000:
        fmt_time = "%d %s" % (uptime_minutes / 60 /24, 'days')    
    elif uptime_minutes > 300:
        fmt_time = "%d %s" % (uptime_minutes / 60, 'hours')        
    else:
        if uptime_minutes == 0:
            fmt_time = "%d %s" % (uptime_minutes, '< 1 minute')                
        else:    
            fmt_time = "%d %s" % (uptime_minutes, 'minutes')    
    if uptime_minutes > 120:
        printit("%s ** Server Uptime > 2 hours: Start Time = %s Uptime = %s." % (serverid, uptime_start, fmt_time))    
        return 0
      
    print "start=%s   uptime_mins=%d" % (uptime_start, uptime_minutes)  
    body = body + "%s (UPTIME Notification) *** Server Start Time = %s Server Uptime = %s" % (serverid, uptime_start, fmt_time)
               
    to      = 'pgdude@noreply.com michael.vitale@verizon.com'
    from_   = 'pgdude@noreply.com'
    subject = 'Server %s : `date`  Server Uptime < 2 hours.  Server Start = %s  Server Uptime = %s' % (serverid, uptime_start, fmt_time)
    rc = send_mail(to, from_, subject, body)
    if rc <> 0:
        printit("mail error")
        return 1
    printit("%s ** Server uptime less than 2 hours. Server Start = %s  Server Uptime = %s" % (serverid, uptime_start, fmt_time))
    return 0    

def check_bloat():
    '''
    SELECT current_database() as databasename, schemaname, tblname as "relation", pretty_size, reltuples::bigint, bs*tblpages AS real_size, (tblpages-est_tblpages)*bs AS extra_size, CASE WHEN tblpages - est_tblpages > 0  THEN round(100 * (tblpages - est_tblpages)/tblpages::float)  ELSE 0  END AS extra_ratio, fillfactor, (tblpages-est_tblpages_ff)*bs AS bloat_size, CASE WHEN tblpages - est_tblpages_ff > 0    THEN round(100 * (tblpages - est_tblpages_ff)/tblpages::float)  ELSE 0 END AS bloat_ratio,is_na::varchar FROM (SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages, ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + ceil( toasttuples / 4 ) AS est_tblpages_ff, tblpages, fillfactor, bs, tblid, schemaname, tblname, pretty_size, heappages, toastpages, reltuples,is_na FROM (SELECT ( 4 + tpl_hdr_size + tpl_data_size + (2*ma) - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END - CASE WHEN ceil(tpl_data_size)::int%ma = 0 THEN ma ELSE ceil(tpl_data_size)::int%ma END) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages, toastpages, reltuples, toasttuples, bs, page_hdr, tblid, schemaname, tblname, pretty_size, fillfactor, is_na FROM (SELECT tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples, pg_size_pretty(pg_total_relation_size(quote_ident(ns.nspname) || '.' || quote_ident(tbl.relname))) as pretty_size, tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages,  coalesce(toast.reltuples, 0) AS toasttuples, coalesce(substring(array_to_string(tbl.reloptions, ' ') FROM '%fillfactor=#"__#"%' FOR '#')::smallint, 100) AS fillfactor, current_setting('block_size')::numeric AS bs, CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma, 24 AS page_hdr, 23 + CASE WHEN MAX(coalesce(null_frac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size, sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS tpl_data_size, bool_or(att.atttypid = 'pg_catalog.name'::regtype) AS is_na FROM pg_attribute AS att JOIN pg_class AS tbl ON att.attrelid = tbl.oid JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace JOIN pg_stats AS s ON s.schemaname=ns.nspname AND ns.nspname not in ('pg_catalog') AND s.tablename = tbl.relname AND s.inherited=false AND s.attname=att.attname LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid WHERE att.attnum > 0  AND NOT att.attisdropped AND tbl.relkind = 'r' GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12, tbl.relhasoids ORDER BY 2,3) AS s) AS s2) AS s3 WHERE bs*tblpages > 16384 order by bloat_ratio desc limit 10;
    databasename | schemaname |            relation             | pretty_size | reltuples |  real_size   | extra_size  | extra_ratio | fillfactor | bloat_size  | bloat_ratio | is_na
    --------------+------------+---------------------------------+-------------+-----------+--------------+-------------+-------------+------------+-------------+-------------+-------
    gwyvlati     | prestaging | base_loc_update                 | 216 kB      |         2 |       180224 |      172032 |          95 |        100 |      172032 |          95 | false
    
    SELECT pg_size_pretty(sum(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))) FROM pg_tables WHERE schemaname = 'prestaging' AND tablename = 'base_loc_update';
    '''
    sql = \
        "SELECT current_database() as databasename, schemaname, tblname relation, pretty_size, reltuples::bigint, bs*tblpages AS real_size, (tblpages-est_tblpages)*bs AS extra_size, " \
        "CASE WHEN tblpages - est_tblpages > 0  THEN round(100 * (tblpages - est_tblpages)/tblpages::float)  ELSE 0  END AS extra_ratio, fillfactor, (tblpages-est_tblpages_ff)*bs AS bloat_size, " \
        "CASE WHEN tblpages - est_tblpages_ff > 0    THEN round(100 * (tblpages - est_tblpages_ff)/tblpages::float)  ELSE 0 END AS bloat_ratio,is_na::varchar FROM " \
        "(SELECT ceil( reltuples / ( (bs-page_hdr)/tpl_size ) ) + ceil( toasttuples / 4 ) AS est_tblpages, ceil( reltuples / ( (bs-page_hdr)*fillfactor/(tpl_size*100) ) ) + " \
        "ceil( toasttuples / 4 ) AS est_tblpages_ff, tblpages, fillfactor, bs, tblid, schemaname, tblname, pretty_size, heappages, toastpages, reltuples,is_na FROM " \
        "(SELECT ( 4 + tpl_hdr_size + tpl_data_size + (2*ma) - CASE WHEN tpl_hdr_size%ma = 0 THEN ma ELSE tpl_hdr_size%ma END - CASE WHEN ceil(tpl_data_size)::int%ma = 0 " \
        "THEN ma ELSE ceil(tpl_data_size)::int%ma END) AS tpl_size, bs - page_hdr AS size_per_block, (heappages + toastpages) AS tblpages, heappages, toastpages, reltuples, toasttuples, " \
        "bs, page_hdr, tblid, schemaname, tblname, pretty_size, fillfactor, is_na FROM (SELECT tbl.oid AS tblid, ns.nspname AS schemaname, tbl.relname AS tblname, tbl.reltuples,  " \
        "pg_size_pretty(pg_total_relation_size(quote_ident(ns.nspname) || '.' || quote_ident(tbl.relname))) as pretty_size, tbl.relpages AS heappages, coalesce(toast.relpages, 0) AS toastpages, " \
        "coalesce(toast.reltuples, 0) AS toasttuples, coalesce(substring(array_to_string(tbl.reloptions, ' ') FROM '%fillfactor=#\"__#\"%' FOR '#')::smallint, 100) AS fillfactor, " \
        "current_setting('block_size')::numeric AS bs, CASE WHEN version()~'mingw32' OR version()~'64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS ma, 24 AS page_hdr, 23 +  " \
        "CASE WHEN MAX(coalesce(null_frac,0)) > 0 THEN ( 7 + count(*) ) / 8 ELSE 0::int END + CASE WHEN tbl.relhasoids THEN 4 ELSE 0 END AS tpl_hdr_size,  " \
        "sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024) ) AS tpl_data_size, bool_or(att.atttypid = 'pg_catalog.name'::regtype) AS is_na FROM pg_attribute AS att JOIN pg_class  " \
        "AS tbl ON att.attrelid = tbl.oid JOIN pg_namespace AS ns ON ns.oid = tbl.relnamespace JOIN pg_stats AS s ON s.schemaname=ns.nspname AND ns.nspname not in ('pg_catalog') AND s.tablename =  " \
        "tbl.relname AND s.inherited=false AND s.attname=att.attname LEFT JOIN pg_class AS toast ON tbl.reltoastrelid = toast.oid WHERE att.attnum > 0  AND NOT att.attisdropped AND tbl.relkind = 'r' " \
        "GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12, tbl.relhasoids ORDER BY 2,3) AS s) AS s2) AS s3 WHERE bs*tblpages > 16384 order by bloat_ratio desc limit 20"
    try:
        cur.execute(sql)
    except psycopg2.Error as e:
        printit ("Bloat Select Error: %s" % (e))
        return 1

    rows = cur.fetchall()
    if len(rows) == 0:
        printit ("No bloat above threshold: %d." % (bloatpct))

    cnt = 0
    lqcnt = 0
    body = ''
    for row in rows:
        cnt = cnt + 1
        dbname      = row[0]
        schemaname  = row[1]
        tablename   = row[2]
        pretty_size = row[3]
        reltuples   = int(row[4])
        real_size   = row[5]
        extra_size  = row[6]    
        extra_ratio = row[7]    
        fillfactor  = row[8]   
        bloat_size  = int(row[9])
        bloat_ratio = int(row[10])
        is_na       = row[11]    

        if bloat_ratio < bloatpct:
            continue

        lqcnt = lqcnt + 1
        body = body + "%s (BLOAT) ** db=%s  table=%s.%s  bloatpct=%s  bloat_size=%s  pretty_size=%s  reltuples=%d  real_size=%s  extra_size=%s  extra_ratio=%s  fillfactor=%s  is_na=%s\n\n" \
                   % (serverid, dbname, schemaname, tablename, bloat_ratio, bloat_size, pretty_size, reltuples, real_size, extra_size, extra_ratio, fillfactor, is_na)

    if notify and lqcnt > 0:
        to      = 'pgdude@noreply.com michael.vitale@verizon.com'
        from_   = 'pgdude@noreply.com'
        #subject = 'Server `hostname` : `date`  test subject'
        subject = 'Server %s : `date`  bloat=%d ' % (serverid, lqcnt)
        rc = send_mail(to, from_, subject, body)
        if rc <> 0:
            printit("mail error")
            return 1
    if lqcnt > 0:        
        printit("%s ** Bloat Detected = %d" % (serverid, lqcnt))
        printit("%s" % body)
    else:
        printit("%s ** No bloat > %d%%." % (serverid,bloatpct))    
    return 0
    
    
####################
# MAIN ENTRY POINT #
####################

# Register the signal handler for CNTRL-C logic
signal.signal(signal.SIGINT, signal_handler)

#while True:
#    print('Waiting...')
#    time.sleep(5)

total_whatever = 0
tablist = []

# Setup up the argument parser
# parser = OptionParser("PostgreSQL Vacumming Tool", add_help_option=False)
'''
# parser = OptionParser("PostgreSQL Vacumming Tool", add_help_option=True)
parser.add_option("-r", "--dryrun", dest="dryrun",   help="dry run", default=False, action="store_true", metavar="DRYRUN")
parser.add_option("-H", "--host",   dest="hostname", help="host name",      type=str, default="localhost",metavar="HOSTNAME")
parser.add_option("-d", "--dbname", dest="dbname",   help="database name",  type=str, default="",metavar="DBNAME")
parser.add_option("-u", "--dbuser", dest="dbuser",   help="database user",  type=str, default="postgres",metavar="DBUSER")
parser.add_option("-m", "--schema",dest="schema",    help="schema",         type=str, default="",metavar="SCHEMA")
parser.add_option("-p", "--dbport", dest="dbport",   help="database port",  type=int, default="5432",metavar="DBPORT")
parser.add_option("-a", "--action", dest="action",   help="action",         type=str, default="",metavar="ACTION")
parser.add_option("-c", "--cpumaxpct", dest="cpumaxpct",   help="max cpu load acceptable",  type=int, default=100,metavar="CPUMAXPCT")
(options,args) = parser.parse_args()
'''
parser = argparse.ArgumentParser("PostgreSQL Vacumming Tool", add_help=True)
parser.add_argument("-r", "--dryrun", dest="dryrun",          help="dry run",                  default=False, action="store_true")
parser.add_argument("-n", "--notify",  dest="notify",         help="notify",                   default=False, action="store_true")
parser.add_argument("-H", "--host",   dest="hostname",        help="host name",                type=str, default="localhost",metavar="HOSTNAME")
parser.add_argument("-d", "--dbname", dest="dbname",          help="database name",            type=str, default="",metavar="DBNAME")
parser.add_argument("-u", "--dbuser", dest="dbuser",          help="database user",            type=str, default="postgres",metavar="DBUSER")
parser.add_argument("-m", "--schema",dest="schema",           help="schema",                   type=str, default="",metavar="SCHEMA")
parser.add_argument("-p", "--dbport", dest="dbport",          help="database port",            type=int, default="5432",metavar="DBPORT")
parser.add_argument("-s", "--serverid", dest="serverid",      help="server id",                type=str, default="",metavar="SERVERID")
parser.add_argument("-a", "--action", dest="action",          help="action",                   type=str, default="",metavar="ACTION")
parser.add_argument("-c", "--cpus", dest="cpus",              help="expected CPUs for host",   type=int, default=0,metavar="CPUS")
parser.add_argument("-t", "--maxmin", dest="maxmin",          help="max minutes",              type=int, default=30,metavar="MAXMIN")
parser.add_argument("-x", "--cpumaxpct", dest="cpumaxpct",    help="max cpu load acceptable",  type=int, default=250,metavar="CPUMAXPCT")
parser.add_argument("-q", "--maxquerylen", dest="maxquerylen",help="max query length",         type=int, default=9999,metavar="MAXQUERYLEN")
parser.add_argument("-b", "--bloatpct", dest="bloatpct",          help="max bloat pct allowed",type=int, default=50,metavar="BLOATPCT")
args = parser.parse_args()

# pg_report.py -H localhost -d testing -p 5432 -u postgres --schema public --dryrun --action load instanceCPUs 8 --cpumaxpct 100


dryrun = False
if args.dryrun:
    dryrun = True

if args.hostname == "":
    printit("Hostname must be provided.")
    sys.exit(1)

if args.dbname == "":
    printit("DB Name must be provided.")
    sys.exit(1)    

# if args.maxsize <> -1:
dbname    = args.dbname
hostname  = args.hostname
dbport    = args.dbport
dbuser    = args.dbuser
schema    = args.schema
action    = args.action.lower()
cpus      = args.cpus
serverid  = args.serverid
notify    = args.notify
cpumaxpct = args.cpumaxpct
maxmin    = args.maxmin
maxquerylen = int(args.maxquerylen)
bloatpct  = int(args.bloatpct)

if serverid == '':
    printit("ServerID missing. You must provide this parameter, --s <my server id>.")
    sys.exit(1)        
if action == 'load':

    if cpus == 0:
        printit("Load Testing: CPUs expected for target host must be provided.")
        sys.exit(1) 
elif action == 'longqueries':
    pass
elif action == 'blockedqueries':
    pass    
elif action == 'uptime':
    pass        
elif action == 'bloat':
    if bloatpct > 99:
        printit("Bloat Notification: Max Bloat pct is 99, %d provided." % (bloatpct))
        sys.exit(1)     
    pass            
else:
    printit("Invalid action.  Valid actions are: load, longqueries, blockedqueries, uptime, bloat")
    sys.exit(1)

#printit ("version: *** %s ***  Parms: host:%s dbname=%s schema=%s dbuser=%s dbport=%d action: %s cpumaxpct: %d cpus=%d serverid: %s maxmin=%d notify=%r maxquerylen=%d" \
#        % (version, hostname, dbname, schema, dbuser, dbport, action, cpumaxpct, cpus, serverid, maxmin, notify, maxquerylen ))

print version
    
# Connect
try:
    connstr = "dbname=%s port=%d user=%s host=%s application_name=%s" % (dbname, dbport, dbuser, hostname, 'pg_report' )
    conn = psycopg2.connect(connstr)
except psycopg2.Error as e:
    printit ("Database Connection Error: %s" % (e))
    sys.exit(1)

#printit ("connected to database successfully.")

# to run vacuum through the psycopg2 driver, the isolation level must be changed.
old_isolation_level = conn.isolation_level
conn.set_isolation_level(0)

# Open a cursor to perform database operation
cur = conn.cursor()

if action == 'load':
    rc = check_load_db(cpumaxpct)
    if rc <> 0:
        printit("load check error: %d" % rc)
        conn.close()
        sys.exit (1)

elif action == 'longquery':
    rc = check_longqueries()
    if rc <> 0:
        printit("longqueries check error: %d" % rc)
        conn.close()
        sys.exit (1)
        
elif action == 'blockedqueries':
    rc = check_blockedqueries()
    if rc <> 0:
        printit("blockqueries check error: %d" % rc)
        conn.close()
        sys.exit (1)        
        
elif action == 'uptime':
    rc = check_uptime()
    if rc <> 0:
        printit("ServerUptime check error: %d" % rc)
        conn.close()
        sys.exit (1)                
        
elif action == 'bloat':
    rc = check_bloat()
    if rc <> 0:
        printit("Bloat check error: %d" % rc)
        conn.close()
        sys.exit (1)                        

# Close communication with the database
conn.close()
#printit ("Closed the connection and exiting normally.")
sys.exit(0)
