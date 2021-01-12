#!/usr/bin/env python2 
#!/usr/bin/env python3 
#!/usr/bin/env python
#!/usr/bin/python
###############################################################################
### COPYRIGHT NOTICE FOLLOWS.  DO NOT REMOVE
###############################################################################
### Copyright (c) 2016 - 2021 SQLEXEC LLC
###
### Permission to use, copy, modify, and distribute this software and its
### documentation for any purpose, without fee, and without a written agreement
### is hereby granted, provided that the above copyright notice and this paragraph
### and the following two paragraphs appear in all copies.
###
### IN NO EVENT SHALL SQLEXEC LLC BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
### INDIRECT SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
### ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF
### SQLEXEC LLC HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
###
### SQLEXEC LLC SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
### LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
### PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS,
### AND SQLEXEC LLC HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
### ENHANCEMENTS, OR MODIFICATIONS.
###
###############################################################################
#
# Original Author: Michael Vitale, michaeldba@sqlexec.com
#
# Description: This python utility program performs PostgreSQL maintenance tasks.
#
# Inputs: all fields are optional except database.
# -h <hostname or IP address>
# -d <database>
# -n <schema>
# -p <PORT>
# -U <db user>
# -m [html format flag]
# -r [dry run flag]
# -v [verbose output flag, mostly used for debugging]
#
# Examples: run report on entire test database and output in web format
# ./pg_report.py -d test --html
#
# Requirements:
#  1. python 2.6 or 2.7
#  2. psql client
#  3. psutil for windows only: https://pypi.python.org/pypi?:action=display&name=psutil#downloads
#      (fyi for gettting it on linux but not required: apt-get install python-psutil or yum install python-psutil)
#
# Download: git clone https://github.com/commandprompt/pg_report.git pg_report
#
# Assumptions:
# 1. db user defaults to postgres if not provided as parameter.
# 2. Max rows defaults to 10 million if not provided as parameter
# 3. Password must be in local .pgpass file or client authentication changed to trust or peer
# 4. psql must be in the user's path
# 6. Load detection assumes that you are running this script from the database host.
#
# Cron Job Info:
#    View cron job output: view /var/log/cron
#    source the database environment: source ~/db_catalog.ksh
#    Example cron job that does smart vacuum freeze commands for entire database every Saturday at 4am:
#    * 4 * * 6 /usr/bin/python /var/lib/pgsql/pg_tools/pg_report.py -d evergreen -p 5432 --html >> /var/lib/pgsql/pgtools/pg_report_`/bin/date +'\%Y\%m\%d'`.log 2>&1
#
# NOTE: You may have to source the environment variables file in the crontab to get this program to work.
#          #!/bin/bash
#          source /home/user/.bash_profile
#
# Report logic:
#  1. Get database conflicts, deadlocks, and temp_files.
#  2. Unused indexes are identified where there are less than 20 index scans and thee size of the table is > 100 MB.
#  3. Bloated tables/indexes are identified where at least 20% of the table/index is bloated or the wasted bytes is > 1GB.
#  4. See if archiving is getting behind by more than 1000 WAL files.
#  5. Contrast PG memory configuration to recommended ones
#  6. Identify orphaned large objects.
#  7. List tables getting close to transaction wraparound (more than halfway to max freeze threshold).
#  8. list tables that have not been analyzed or vacuumed in the last 60 days or whose size has grown significantly.
#
# TODOs:
#
#
# History:
# who did it         Date           did what
# ==========         =========      ==============================
# Michael Vitale     01/12/2016     Original coding using python 2.7.x on windows 8.1 and ubuntu 14.04 (pg 9.4)
# Michael Vitale     01/13/2016     Finished porting code from bash script, pg_refreshstats.sh
# Michael Vitale     01/14/2016     First crack at incorporated logic for report action.
# Michael Vitale     01/17/2016     Implemented report output in html
# Michael Vitale     01/18/2016     Fixed a bunch of bugs with html reporting
# Michael Vitale     01/20/2016     Removed linux dependency on psutils module.
#                                   Enhanced unused indexes report to query slaves if available
# Michael Vitale     01/21/2016     Fixed bugs, normalized html output, added connection and locking report
# Michael Vitale     01/23/2016     Reworked html output to display health check at top of page and lists at the bottom.
# Michael Vitale     01/25/2016     Added more health check items: writers, network standbys.  Implemented logic related to
#                                   checkpoint, background and backend writers and the pg_stat_bgwriter table.
# Michael Vitale     01/27/2016     loop over tables being worked on, instead of executing
#                                   them in batch: analyze, vacuum analyze, and vacuum freeze actions.
# Michael Vitale     01/28/2016     Fixed python piping to use bash as default shell
# Michael Vitale     10/04/2017     Fix queries based on PG versions since 9.5 (ie 9.6 and 10)
# Michael Vitale     10/16/2020     Qualify vacuumlo command with port number. It had assumed default, 5432
# Michael Vitale     12/08/2020     Majore rewrite: converted pg_maint health check portion to replace old pg_report that also did vacuum actions
# Michael Vitale     12/09/2020     Added new functionality and made changes to be more compatible across Python versions 2 and 3:
#                                   Changed "<>" to <!=>   
#                                   Changed print "whatever" to print ("whatever") 
#                                   Removed Psycopg2 exception handling and replaced with general one.
#                                   shell command can return an empty byte string in python3--> b'', so check for that as well.
# Michael Vitale     01/01/2021     v2.1 new features: Connection time avg, PG major/minor version evaluation
#                                   v2.1 changes:  store major and minor pg versions, not just major version for new logic coming
#                                   v2.1 fixes: Fixed check against 9.6 version for wal directoy location.
# Michael Vitale     01/12/2021     v2.1 fix: Heroku instances should be treated like rds ones
################################################################################################################
import string, sys, os, time
#import datetime
from datetime import datetime
from datetime import date

import tempfile, platform, math
from decimal import *
import smtplib
import subprocess
from subprocess import Popen, PIPE
from optparse  import OptionParser
import getpass

#############################################################################################
#globals
SUCCESS   = 0
ERROR     = -1
ERROR2    = -2
ERROR3    = -3
WARNING   = -4
DEFERRED  = 1
NOTICE    = 2
TOOLONG   = 3
HIGHLOAD  = 4
DESCRIPTION="This python utility program performs a basic health check for a PostgreSQL cluster."
VERSION    = 2.1
PROGNAME   = "pg_report"
ADATE      = "January 1, 2021"
PROGDATE   = "2021-01-01"
MARK_OK    = "[ OK ]  "
MARK_WARN  = "[WARN]  "

#############################################################################################
########################### class definition ################################################
#############################################################################################
class maint:
    def __init__(self):
    
        self.dateprogstr       = PROGDATE
        self.dateprog          = datetime.strptime(PROGDATE, "%Y-%m-%d")
        self.datenowstr        = datetime.now().strftime("%Y-%m-%d")
        self.datenow           = datetime.today()
        self.datediff          = self.datenow - self.dateprog
        
        self.action            = ''
        self.dbhost            = ''
        self.dbport            = 5432
        self.dbuser            = ''
        self.database          = ''
        self.dry_run           = False
        self.verbose           = False
        self.connected         = False

        self.fout              = ''
        self.connstring        = ''

        self.actstring         = ''
        self.schemaclause      = ' '
        self.pid               = os.getpid()
        self.opsys             = ''
        self.tempdir           = tempfile.gettempdir()
        self.workfile          = ''
        self.workfile_deferred = ''
        self.tempfile          = ''
        self.reportfile        = ''
        self.dir_delim         = ''
        self.totalmemGB        = -1
        self.pgbindir          = ''
        self.pgversionmajor    = Decimal('0.0')
        self.pgversionminor    = '0.0'        
        self.html_format       = False
        self.programdir        = ''
        self.imageURL          = "https://cloud.githubusercontent.com/assets/12436545/12725212/7a1a27be-c8df-11e5-88a6-4e6a88004daa.jpg"

        self.slaves            = []
        self.slavecnt          = 0
        self.in_recovery       = False
        self.bloatedtables     = False
        self.unusedindexes     = False
        self.freezecandidates  = False
        self.analyzecandidates = False
        self.timestartmins     = time.time() / 60

        # db config stuff
        self.archive_mode      = ''
        self.max_connections   = -1
        self.datadir           = ''
        self.shared_buffers    = -1
        self.work_mem          = -1
        self.maint_work_mem    = -1
        self.eff_cache_size    = -1
        self.shared_preload_libraries = ''
        self.pg_type           = 'community'

        self.overcommit_memory = -1
        self.overcommit_ratio  = -1

    ###########################################################
    def set_dbinfo(self, dbhost, dbport, dbuser, database, schema, html_format, dry_run, verbose, argv):
        self.dbhost          = dbhost
        self.dbport          = dbport
        self.dbuser          = dbuser
        self.database        = database
        self.schema          = schema
        self.html_format     = html_format
        self.dry_run         = dry_run
        self.verbose         = verbose

        # process the schema or table elements
        total   = len(argv)
        cmdargs = str(argv)

        if os.name == 'posix':
            self.opsys = 'posix'
            self.dir_delim = '/'
        elif os.name == 'nt':
            self.opsys = 'nt'
            self.dir_delim = '\\'
        else:
            return ERROR, "Unsupported platform."

        self.workfile          = "%s%s%s_stats.sql" % (self.tempdir, self.dir_delim, self.pid)
        self.workfile_deferred = "%s%s%s_stats_deferred.sql" % (self.tempdir, self.dir_delim, self.pid)
        self.tempfile          = "%s%s%s_temp.sql" % (self.tempdir, self.dir_delim, self.pid)
        if self.html_format:
            self.reportfile        = "%s%s%s_report.html" % (self.tempdir, self.dir_delim, self.pid)
        else:    
            self.reportfile        = "%s%s%s_report.txt" % (self.tempdir, self.dir_delim, self.pid)
        

        # construct the connection string that will be used in all database requests
        # do not provide host name and/or port if not provided
        if self.dbhost != '':
            self.connstring = " -h %s " % self.dbhost
        if self.database != '':
            self.connstring += " -d %s " % self.database
        if self.dbport != '':
            self.connstring += " -p %s " % self.dbport
        if self.dbuser != '':
            self.connstring += " -U %s " % self.dbuser
        if self.schema != '':
            self.schemaclause = " and n.nspname = '%s' " % self.schema

        if self.verbose:
            print ("The total numbers of args passed to the script: %d " % total)
            print ("Args list: %s " % cmdargs)
            print ("connection string: %s" % self.connstring)

        self.programdir = sys.path[0]

        # Make sure psql is in the path
        if self.opsys == 'posix':
            cmd = "which psql"
        else:
            # assume windows
            cmd = "where psql"
        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            errors = "Unable to determine if psql is in path. rc=%d results=%s" % (rc,results)
            return rc, errors
        if 'psql' not in results:
            msg = "psql must be in the path. rc=%d, results=%s" % (rc, results)
            return ERROR, msg

        pos = results.find('psql')
        if pos > 0:
            self.pgbindir = results[0:pos]

        rc, results = self.get_configinfo()
        if rc != SUCCESS:
            errors = "rc=%d results=%s" % (rc,results)
            return rc, errors

        # get total memory  total memory is in bytes
        self.totalmemGB = self.get_physicalmem()

        self.overcommit_memory, self.overcommit_ratio = self.get_kernelmemorycapacity()

        # get pg bind directory from pg_config
        rc, results = self.get_pgbindir()
        if rc != SUCCESS:
            errors = "rc=%d results=%s" % (rc,results)
            return rc, errors

        rc, results = self.get_pgversion()
        if rc != SUCCESS:
            return rc, results

        # Validate parameters
        rc, errors = self.validate_parms()
        if rc != SUCCESS:
            return rc, errors

        return SUCCESS, ''

    ###########################################################
    def validate_parms(self):

        if self.database == '':
            return ERROR, "Database not provided."

        return SUCCESS, ""


    ###########################################################
    def get_physicalmem(self):

        if self.opsys == 'posix':
            cmd = "free -g | grep Mem: | /usr/bin/awk '{ total=$2; } END { print \"total=\" total  }'"
            rc, results = self.executecmd(cmd, True)
            if rc != SUCCESS:
                errors = "unable to get Total Physical Memory.  rc=%d %s\n" % (rc, results)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            #print ("rc=%d results=%s" % (rc,results))
            results = results.split('=')
            totalmem_prettyGB = int(results[1].strip())
        else:
            # must be windows, nt
            from psutil import virtual_memory
            mem = virtual_memory()
            totalmem_prettyGB = mem.total / (1024*1024*1024)

        if self.verbose:
            print (" total physical memory: %s GB" % totalmem_prettyGB)

        return totalmem_prettyGB

    ###########################################################
    def get_kernelmemorycapacity(self):

        overcommit_memory = -1
        overcommit_ratio  = -1
        if self.opsys == 'posix':
            cmd = "cat /proc/sys/vm/overcommit_memory"
            rc, results = self.executecmd(cmd, True)
            if rc != SUCCESS:
                errors = "unable to get overcommit_memory.  rc=%d %s\n" % (rc, results)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            overcommit_memory = int(results[0].strip())
            
            cmd = "cat /proc/sys/vm/overcommit_ratio"
            rc, results = self.executecmd(cmd, True)
            if rc != SUCCESS:
                errors = "unable to get overcommit_ratio.  rc=%d %s\n" % (rc, results)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            overcommit_ratio = int(results.strip())
        else:
            # must be windows, nt
            # and we don't do windows at the current time
            return -1, -1

        if self.verbose:
            print ("overcommit_memory: %s  overcommit_ratio: %s" % (overcommit_memory, overcommit_ratio))

        return overcommit_memory, overcommit_ratio


    ###########################################################
    def cleanup(self):
        if self.connected:
            # do something here later if we enable a db driver
            self.connected = false
        # print ("deleting temp file: %s" % self.tempfile)
        try:
            os.remove(self.tempfile)
        except OSError:
            pass
        return

    ###########################################################
    def getnow(self):
        now = datetime.now()
        adate = str(now)
        parts = adate.split('.')
        return parts[0]

    ###########################################################
    def getfilelinecnt(self, afile):
        return sum(1 for line in open(afile))

    ###########################################################
    def convert_humanfriendly_to_MB(self, humanfriendly):

        # assumes input in form: 10GB, 500 MB, 200 KB, 1TB
        # returns value in megabytes
        hf = humanfriendly.upper()
        valueMB = -1
        if 'TB' in (hf):
            pos = hf.find('TB')
            valueMB = int(hf[0:pos]) * (1024*1024)
        elif 'GB' in (hf):
            pos = hf.find('GB')
            value = hf[0:pos]
            valueMB = int(hf[0:pos]) * 1024
        elif 'MB' in (hf):
            pos = hf.find('MB')
            valueMB = int(hf[0:pos]) * 1
        elif 'KB' in (hf):
            pos = hf.find('KB')
            valueMB = round(float(hf[0:pos]) / 1024, 2)

        valuefloat = "%.2f" % valueMB
        return Decimal(valuefloat)


    ###########################################################
    def writeout(self,aline):
        if self.fout != '':
            aline = aline + "\r\n"
            self.fout.write(aline)
        else:
            # default to standard output
            print (aline)
        return

    ###########################################################
    def get_configinfo(self):

        sql = "show all"

        cmd = "psql %s -t -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            # let calling function report the error
            errors = "Unable to get config info: %d %s\nsql=%s\n" % (rc, results, sql)
            #aline = "%s" % (errors)
            #self.writeout(aline)
            return rc, errors

        f = open(self.tempfile, "r")
        lineno = 0
        count  = 0
        for line in f:
            lineno = lineno + 1
            aline = line.strip()
            if len(aline) < 1:
                continue

            fields = aline.split('|')
            name = fields[0].strip()
            setting = fields[1].strip()
            #print ("name=%s  setting=%s" % (name, setting))

            if name == 'data_directory':
                self.datadir = setting
                # for pg rds version, 9.6,  "show all" command does not have shared_preload_libraries! so rely on data_directory instead
                if 'rdsdbdata' in self.datadir:
                    self.pg_type = 'rds'                
                # heroku indicator using aws in the background
                elif self.datadir == '/database':
                    self.pg_type = 'rds'                
            elif name == 'archive_mode':
                self.archive_mode = setting
            elif name == 'max_connections':
                self.max_connections = int(setting)
            elif name == 'shared_buffers':
                # shared_buffers in 8kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                # self.shared_buffers = int(setting) / 8192
                rc = self.convert_humanfriendly_to_MB(setting)
                self.shared_buffers = rc
            elif name == 'maintenance_work_mem':
                # maintenance_work_mem in kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                # self.maint_work_mem = int(setting) / 1024
                rc = self.convert_humanfriendly_to_MB(setting)
                self.maint_work_mem = rc
            elif name == 'work_mem':
                # work_mem in kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                #self.work_mem = int(setting) / 1024
                rc = self.convert_humanfriendly_to_MB(setting)
                self.work_mem = rc
            elif name == 'effective_cache_size':
                # effective_cache_size in 8 kilobytes units from select from pg_settings, so convert to megabytes, but show gives user friendly form (10GB, 10MB, 10KB, etc.)
                rc = self.convert_humanfriendly_to_MB(setting)
                self.eff_cache_size = rc
            elif name == 'shared_preload_libraries':
                # we only care that it is loaded, not necessarily created
                # for pg rds version, 9.6,  "show all" command does not have shared_preload_libraries! so rely on data_directory instead
                self.shared_preload_libraries = setting  
                if 'rdsutils' in self.shared_preload_libraries:
                    self.pg_type = 'rds'
            elif name == 'rds.extensions':
                self.pg_type = 'rds'

        f.close()

        if self.verbose:
            print ("shared_buffers = %d  maint_work_mem = %d  work_mem = %d  shared_preload_libraries = %s" % (self.shared_buffers, self.maint_work_mem, self.work_mem, self.shared_preload_libraries))

        return SUCCESS, results

    ###########################################################
    def executecmd(self, cmd, expect):
        if self.verbose:
            print ("executecmd --> %s" % cmd)

        # NOTE: try and catch does not work for Popen
        try:
            # Popen(args, bufsize=0, executable=None, stdin=None, stdout=None, stderr=None, preexec_fn=None, close_fds=False, shell=False, cwd=None, env=None, universal_newlines=False, startupinfo=None, creationflags=0)
            if self.opsys == 'posix':
                p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, executable="/bin/bash")
            else:
                p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
            values2, err2 = p.communicate()

        except exceptions.OSError as e:
            print ("exceptions.OSError Error",e)
            return ERROR, "Error(1)"
        except BaseException as e:
            print ("BaseException Error",e)
            return ERROR, "Error(2)"
        except OSError as e:
            print ("OSError Error", e)
            return ERROR, "Error(3)"
        except RuntimeError as e:
            print ("RuntimeError", e)
            return ERROR, "Error(4)"
        except ValueError as e:
            print ("Value Error", e)
            return ERROR, "Error(5)"
        except Exception as e:
            print ("General Exception Error", e)
            return ERROR, "Error(6)"
        except:
            print ("Unexpected error:", sys.exc_info()[0])
            return ERROR, "Error(7)"

        if err2 is None or len(err2) == 0:
            err = ""
        else:
            # python 3 returns values and err in byte format so convert accordingly
            err = bytes(err2).decode('utf-8')
            
        if values2 is None or len(values2) == 0:
            values = ""
        else:
            # python 3 returns values and err in byte format so convert accordingly
            values = bytes(values2).decode('utf-8')

        values = values.strip()
                
        rc = p.returncode
        if self.verbose:
            print ("rc=%d  values=***%s***  errors=***%s***" % (rc, values, err))

        if rc == 1 or rc == 2:
            return ERROR2, err
        elif rc == 127:
            return ERROR2, err
        elif err != "":
            # do nothing since INFO information is returned here for analyze commands
            # return ERROR, err
            return SUCCESS, err
        elif values == "" and expect == True:
            return ERROR2, values
        elif rc != SUCCESS:
            # print or(stderr_data)
            return rc, err
        elif values == "" and expect:
            return ERROR3, 'return set is empty'
        else:
            return SUCCESS, values


    ###########################################################
    def get_pgversion(self):

        # v 2.1 fix: expected output --> 10.15-10.
        #sql = "select substring(foo.version from 12 for 3) from (select version() as major) foo, substring(version(), 12, position(' ' in substring(version(),12))) as minor"
        #sql = "select substring(version(), 12, position(' ' in substring(version(),12)))"
        sql = "select  trim(substring(version(), 12, position(' ' in substring(version(),12)))) || '-' || substring(foo.major from 12 for 3)as major  from (select version() as major) foo"
        
        # do not provide host name and/or port if not provided
        cmd = "psql %s -t -c \"%s\" " % (self.connstring, sql)

        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            errors = "%s\n" % (results)
            aline = "%s" % (errors)

            self.writeout(aline)
            return rc, errors

        # with version 10, major version format changes from x.x to x, where x is a 2 byte integer, ie, 10, 11, etc.
        # values = bytes(values2).decode('utf-8')
        results = str(results)
        parsed = results.split('-')
        
        amajor = parsed[1]
        self.pgversionminor = parsed[0]
        
        pos = amajor.find('.')
        if pos == -1:
            # must be a beta or rc candidate version starting at version 10 since the current version is 10rc1
            self.pgversionmajor =  Decimal(amajor[:2])
        else:
            self.pgversionmajor = Decimal(amajor)
        
        # print "majorversion = %.1f  minorversion = %s" % (self.pgversionmajor, self.pgversionminor)            
        return SUCCESS, str(results)

    ###########################################################
    def get_readycnt(self):
        
        # we cannot handle cloud types like AWS RDS
        if self.pg_type == 'rds':
            return SUCCESS, '0'
        
        # version 10 replaces pg_xlog with pg_wal directory
        if self.pgversionmajor > Decimal('9.6'):
            xlogdir = "%s/pg_wal/archive_status" % self.datadir                
        else:
            xlogdir = "%s/pg_xlog/archive_status" % self.datadir        
        
        sql = "select count(*) from (select pg_ls_dir from pg_ls_dir('%s') where pg_ls_dir ~ E'^[0-9A-F]{24}.ready$') as foo" % xlogdir

        # do not provide host name and/or port if not provided
        cmd = "psql %s -t -c \"%s\" " % (self.connstring, sql)

        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            errors = "%s\n" % (results)
            aline = "%s" % (errors)

            self.writeout(aline)
            return rc, errors

        return SUCCESS, str(results)

    ###########################################################
    def get_datadir(self):

        sql = "show data_directory"

        # do not provide host name and/or port if not provided
        cmd = "psql %s -t -c \"%s\" " % (self.connstring, sql)

        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            errors = "%s\n" % (results)
            aline = "%s" % (errors)

            self.writeout(aline)
            return rc, errors

        return SUCCESS, str(results)

    ###########################################################
    def get_pgbindir(self):

        if self.opsys == 'posix':
            cmd = "pg_config | grep BINDIR"
        else:
            cmd = "pg_config | find \"BINDIR\""

        rc, results = self.executecmd(cmd, True)
        if rc != SUCCESS:
            # don't consider failure unless bindir not already populated by "which psql" command that executed earlier
            if self.pgbindir == "":
                errors = "unable to get PG Bind Directory.  rc=%d %s\n" % (rc, results)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            else:
                return SUCCESS, self.pgbindir

        results = results.split('=')
        self.pgbindir   = results[1].strip()

        if self.verbose:
            print ("PG Bind Directory = %s" % self.pgbindir)

        return SUCCESS, str(results)

    ###########################################################
    def get_load(self):

        if self.opsys == 'posix':
            cmd = "cat /proc/cpuinfo | grep processor | wc -l"
            rc, results = self.executecmd(cmd, True)
            if rc != SUCCESS:
                errors = "%s\n" % (results)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            CPUs = int(results)

            cmd = "uptime | grep -ohe 'load average[s:][: ].*' | awk '{ print $5 }'"
            rc, results = self.executecmd(cmd, True)
            if rc != SUCCESS:
                errors = "%s\n" % (results)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            LOAD15=Decimal(results)

            LOADR= round(LOAD15/CPUs * 100,2)
            if self.verbose:
                print ("LOAD15 = %.2f  CPUs=%d LOAD = %.2f%%" % (LOAD15, CPUs, LOADR))

        else:
            # assume windows
            cmd = "wmic cpu get loadpercentage"
            rc, results = self.executecmd(cmd, True)
            if rc != SUCCESS:
                errors = "%s\n" % (results)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            if self.verbose:
                print ("windows load: %d %s" % (rc, results))
            LOAD = results.split('\n')
            LOADR = int(LOAD[1])

        return SUCCESS, str(LOADR)


    ###########################################################
    def check_load(self):

        if self.load_threshold == -1:
            return SUCCESS, ""

        rc, results = self.get_load()
        if rc != SUCCESS:
            return rc, results

        load = Decimal(results)

        if load > self.load_threshold:
            return HIGHLOAD, "Current load (%.2f%%) > Threshold load (%d%%)" % (load, self.load_threshold)
        else:
            return SUCCESS,  "Current load (%.2f%%) < Threshold load (%d%%)" % (load, self.load_threshold)

    ###########################################################
    def get_slaves(self):

        if self.pgversionmajor < Decimal('9.1'):
            # pg_stat_replication table does not exist
            return SUCCESS, ""

        sql = "select count(*) from pg_stat_replication where state = 'streaming'"
        
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get table/index bloat count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors
        cols = results.split('|')            
        self.slavecnt = int(cols[0].strip())

        # Also check whether this cluster is a master or slave
        # self.in_recovery
        sql = "select pg_is_in_recovery()"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get master/slave status: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors
        self.in_recovery = True if results == 't' else False

        return SUCCESS, ""



    ###########################################################
    def initreport(self):

        # get the host name
        if self.dbhost == '':
            # tuples = os.uname()
            tuples = platform.uname()
            hostname = tuples[1]
        else:
            hostname = self.dbhost

        now = str(time.strftime("%c"))

        f = open(self.reportfile, "w")

        contextline = "<H2><p>Host: %s</p><p>Database: %s</p><p>Generated %s</p></H2>\n" % (hostname, self.database, now)
        info = \
            "<!DOCTYPE html>\n" + \
            "<HTML>\n" + \
            "<HEAD>\n" + \
            "<TITLE>PostgreSQL Report Analysis</TITLE>\n" + \
            "</HEAD>\n" + \
            "<style>" + \
            ".table1 {font-size:16px; border:1px solid black; border-collapse:collapse; }" + \
            ".table1 th { color:#000;    text-align:left; border:1px solid black; padding: 5px;}" + \
            ".table1 td { color:#000099; text-align:left; border:1px solid black; padding: 5px;}" + \
            "caption { text-align:left; caption-side: left; }" + \
            "</style>" + \
            "<BODY BGCOLOR=\"FFFFFF\">\n" + \
            "<div id='container'>\n" + \
            "<img src='" + self.imageURL + "' style='float: left;'/>\n" + \
            "<p><H1>PostgreSQL Report Analysis</H1></p>\n" + \
            "</div>\n" + contextline + \
            "<a href=\"https://github.com/MichaelDBA/pg_report\">pg_report</a>  is available on github.\n" + \
            "Send me mail at <a href=\"mailto:michaeldba@sqlexec.com\"> support@sqlexec.com</a>.\n" + \
            "<HR>\n"

        f.write(info)
        f.close()

        return SUCCESS, ""

    ###########################################################
    def finalizereport(self):
        f = open(self.reportfile, "a")
        info = "</BODY>\n</HTML>"
        f.write(info)
        f.close()

        return SUCCESS, ""

    ###########################################################
    def appendreport(self, astring):
        f = open(self.reportfile, "a")
        f.write(astring)
        f.close()

        return SUCCESS, ""

    ###########################################################
    def do_report(self):

        if self.html_format:
            rc,results = self.initreport()
            if rc != SUCCESS:
                return rc, results

        rc, results = self.get_slaves()
        if rc != SUCCESS:
            return rc, results

        # do health checks
        rc, results = self.do_report_healthchecks()
        if rc != SUCCESS:
            return rc, results
        print ("")

        # get pg memory settings
        rc, results = self.do_report_pgmemory()
        if rc != SUCCESS:
            return rc, results

        # get bloated tables and indexes
        rc, results = self.do_report_bloated()
        if rc != SUCCESS:
            return rc, results

        # get unused indexes
        rc, results = self.do_report_unusedindexes()
        if rc != SUCCESS:
            return rc, results

        # See what tables need to be analyzed, vacuumed, etc
        rc, results = self.do_report_tablemaintenance()
        if rc != SUCCESS:
            return rc, results

        if self.html_format:
            rc,results = self.finalizereport()
            if rc != SUCCESS:
                return rc, results

            print ("html report file generated: %s" % self.reportfile)
        else:
            print ("text report file generated: %s" % self.reportfile)
        
        return SUCCESS, ""

    ###########################################################
    def do_report_pgmemory(self):

        if self.pg_type == 'rds':
            # nothing much to report
            return SUCCESS, ""

        # shared_buffers:
        # primitive logic: make shared buffers minimum 4GB or maximum 250GB or 25% of total memory
        # newer versions of PG seem to be more efficient with higher values, so logic is:
        # if pg 9.3 or lower max is 8GB, if pg 9.4 or higher 12 GB max
        if self.pgversionmajor < Decimal('9.3'):
           MAXGB = 8
        else:
           MAXGB = 250
        MINGB = 2
        percent25GB = self.totalmemGB * 0.25
        shared_buffersGB = self.shared_buffers / 1024

        if percent25GB > MAXGB:
            recommended_shared_buffers = MAXGB
        elif percent25GB < MINGB:
            recommended_shared_buffers = percent25GB
        else:
            recommended_shared_buffers = percent25GB
        if self.verbose:
            print ("shared_buffers = %d percent25GB=%d  recommended=%d  totalmemGB=%d" % (self.shared_buffers, percent25GB, recommended_shared_buffers, self.totalmemGB))

        # maintenance_work_mem
        # current pg versions dont perform better with high values, since there is a hard-coded limit of the this memory that will be used,
        # effectively making memory here unavailable for usage elsewhere, so general rule:
        # MIN = 0.128GB, MAX 8 GB
        MIN = 0.128
        MAX = 8
        if self.totalmemGB < 4:
            recommended_maintenance_work_mem = MIN
        elif self.totalmemGB < 8:
            recommended_maintenance_work_mem = 0.256
        elif self.totalmemGB < 16:
            recommended_maintenance_work_mem = 0.512
        elif self.totalmemGB < 32:
            recommended_maintenance_work_mem = 1
        elif self.totalmemGB < 64:
            recommended_maintenance_work_mem = 2
        elif self.totalmemGB < 96:
            recommended_maintenance_work_mem = 4
        else:
            recommended_maintenance_work_mem = MAX

        # work_mem
        # need knowledge of SQL workload to do this effectivly, so for now, consider max connections and total memory
        if self.max_connections < 200:
            if self.totalmemGB < 4:
                recommended_work_mem = 0.016
            elif self.totalmemGB < 8:
                recommended_work_mem = 0.032
            elif self.totalmemGB < 16:
                recommended_work_mem = 0.064
            elif self.totalmemGB < 32:
                recommended_work_mem = 0.128
            elif self.totalmemGB < 64:
                recommended_work_mem = 0.256
            else:
                recommended_work_mem = 0.512
        else:
            if self.totalmemGB < 8:
                recommended_work_mem = 0.016
            elif self.totalmemGB < 16:
                recommended_work_mem = 0.032
            elif self.totalmemGB < 32:
                recommended_work_mem = 0.064
            elif self.totalmemGB < 64:
                recommended_work_mem = 0.128
            else:
                recommended_work_mem = 0.256

        # effective_cache_size: settings shows it in 8kb chunks
        # set it to 85% of memory
        recommended_effective_cache_size = .85 * self.totalmemGB

        totalf = "Current and recommended PG Memory configuration settings are based on a dedicated PG Server with one PG Instance. Total Physical Memory = %s GB" % self.totalmemGB
        print (totalf)
        print ("*** Consider changing these values if they differ significantly ***")

        if self.html_format:
            totalf = "<H4>" + totalf + "</H4>"
            self.appendreport(totalf)
        else:
            self.appendreport(totalf+"\n")
            
        effective_cache_size_f = "%04d GB" % (self.eff_cache_size  / 1024)
        recommended_effective_cache_size_f = "%04d GB" % recommended_effective_cache_size
        print ("effective_cache_size:    %s  recommended: %s" % (effective_cache_size_f, recommended_effective_cache_size_f))

        if self.shared_buffers < 1000:
            # show in MB instead of GB
            shared_buffers_f             = "%04d MB" % self.shared_buffers
            recommended_shared_buffers_f = "%04d MB" % (recommended_shared_buffers * 1024)
            print ("shared_buffers:          %s  recommended: %s" % (shared_buffers_f, recommended_shared_buffers_f))
        else:
            shared_buffers_f             = "%04d GB" % (self.shared_buffers / 1024)
            recommended_shared_buffers_f = "%04d GB" %  recommended_shared_buffers
            print ("shared_buffers:          %s  recommended: %s" % (shared_buffers_f, recommended_shared_buffers_f))

        maintenance_work_mem_f              = "%04d MB" % self.maint_work_mem
        recommended_maintenance_work_mem_f  = "%04d MB" % (recommended_maintenance_work_mem * 1000)
        work_mem_f                          = "%04d MB" % self.work_mem
        recommended_work_mem_f              = "%04d MB" % (recommended_work_mem * 1000)
        print ("maintenance_work_mem:    %s  recommended: %s" % (maintenance_work_mem_f,  recommended_maintenance_work_mem_f ))
        print ("work_mem:                %s  recommended: %s" % (work_mem_f, recommended_work_mem_f ))

        if self.html_format:
            html = "<table border=\"1\">\n" + "<tr>" + "<th align=\"center\">field</th>\n" + "<th align=\"center\">current value</th>\n" + "<th align=\"center\">recommended value</th>\n" + "</tr>\n"
            html += "<tr valign=\"top\">\n" + "<td align=\"left\">effective_cache_size</td>\n" + "<th align=\"center\">" + str(effective_cache_size_f) + "</th>\n" + "<th align=\"center\">" + str(recommended_effective_cache_size_f) + "</th>\n" + "</tr>\n"
            html +="<tr valign=\"top\">\n" + "<td align=\"left\">shared_buffers</td>\n" + "<th align=\"center\">" + str(shared_buffers_f) + "</th>\n" + "<th align=\"center\">" + str(recommended_shared_buffers_f) + "</th>\n" + "</tr>\n"
            html +="<tr valign=\"top\">\n" + "<td align=\"left\">maintenance_work_mem</td>\n" + "<th align=\"center\">" + str(maintenance_work_mem_f) + "</th>\n" + "<th align=\"center\">" + str(recommended_maintenance_work_mem_f) + "</th>\n" + "</tr>\n"
            html +="<tr valign=\"top\">\n" + "<td align=\"left\">work_mem</td>\n" + "<th align=\"center\">" + str(work_mem_f) + "</th>\n" + "<th align=\"center\">" + str(recommended_work_mem_f) + "</th>\n" + "</tr>\n" + "</table>"
            self.appendreport(html)
            self.appendreport("<p><br></p>")
        else:
            self.appendreport("effective_cache_size: %s   recommended: %s\n" % (effective_cache_size_f, recommended_effective_cache_size_f))
            self.appendreport("shared_buffers      : %s   recommended: %s\n" % (shared_buffers_f, recommended_shared_buffers_f))
            self.appendreport("maintenance_work_mem: %s   recommended: %s\n" % (maintenance_work_mem_f, recommended_maintenance_work_mem_f))
            self.appendreport("work_mem            : %s   recommended: %s\n" % (work_mem_f, recommended_work_mem_f))

        return SUCCESS, ""

    ###########################################################
    def do_report_bloated(self):
        '''
         SELECT schemaname, tablename, ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,  iname,   ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat, CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes FROM (SELECT  schemaname, tablename, cc.reltuples, cc.relpages, bs,  CEIL((cc.reltuples*((datahdr+ma- (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,  COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages, COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta FROM ( SELECT   ma,bs,schemaname,tablename,   (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,   (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 FROM ( SELECT schemaname, tablename, hdr, ma, bs, SUM((1-null_frac)*avg_width) AS datawidth, MAX(null_frac) AS maxfracsum,  hdr+( SELECT 1+COUNT(*)/8 FROM pg_stats s2 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename ) AS nullhdr FROM pg_stats s, ( SELECT (SELECT current_setting('block_size')::NUMERIC) AS bs, CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr, CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma FROM (SELECT version() AS v) AS foo ) AS constants  GROUP BY 1,2,3,4,5 ) AS foo) AS rs  JOIN pg_class cc ON cc.relname = rs.tablename  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' LEFT JOIN pg_index i ON indrelid = cc.oid LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid ) AS sml where ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) > 20 OR ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) > 20 or CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END > 10737418240 OR CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END > 10737418240 ORDER BY wastedbytes DESC;
        '''

        if self.bloatedtables == False:
            return SUCCESS, ""

        sql = "SELECT schemaname, tablename, ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,  CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,  iname,   ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat, CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes FROM (SELECT  schemaname, tablename, cc.reltuples, cc.relpages, bs,  CEIL((cc.reltuples*((datahdr+ma- (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,  COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages, COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta FROM ( SELECT   ma,bs,schemaname,tablename,   (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,   (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 FROM ( SELECT schemaname, tablename, hdr, ma, bs, SUM((1-null_frac)*avg_width) AS datawidth, MAX(null_frac) AS maxfracsum,  hdr+( SELECT 1+COUNT(*)/8 FROM pg_stats s2 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename ) AS nullhdr FROM pg_stats s, ( SELECT (SELECT current_setting('block_size')::NUMERIC) AS bs, CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr, CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma FROM (SELECT version() AS v) AS foo ) AS constants  GROUP BY 1,2,3,4,5 ) AS foo) AS rs  JOIN pg_class cc ON cc.relname = rs.tablename  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' LEFT JOIN pg_index i ON indrelid = cc.oid LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid ) AS sml where ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) > 20 OR ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) > 20 or CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END > 10737418240 OR CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END > 10737418240 ORDER BY wastedbytes DESC"
        if self.html_format:
            cmd = "psql %s --html -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)
        else:
            cmd = "psql %s -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get table/index bloat: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if self.html_format:
            self.appendreport("<H4>Bloated tables/indexes are identified where at least 20% of the table/index is bloated or the wasted bytes is > 10 GB.</H4>\n")
        else:    
            self.appendreport("Bloated tables/indexes are identified where at least 20% of the table/index is bloated or the wasted bytes is > 10 GB.\n")

        f = open(self.tempfile, "r")
        lineno = 0
        bloated = 0
        for line in f:
            lineno = lineno + 1
            if self.verbose:
                print ("%d line=%s" % (lineno,line))
            aline = line.strip()
            if len(aline) < 1:
                continue
            elif '(0 rows)' in aline:
                continue

            # bloated table or index
            bloated = bloated + 1
            if self.html_format:
                msg = "%s" % aline
                self.appendreport(msg)
            else:
                self.appendreport("%s\n" % (aline))

        if self.html_format:
            self.appendreport("<p><br></p>")
        f.close()

        return SUCCESS, ""

    ###########################################################
    def do_report_unusedindexes(self):

        # NOTE: no version problems identified yet

        if self.unusedindexes == False:
            return SUCCESS, ""

        # Criteria is indexes that are used less than 20 times and whose table size is > 100MB
        sql="SELECT relname as table, schemaname||'.'||indexrelname AS fqindexname, pg_size_pretty(pg_relation_size(indexrelid)) as total_size, pg_relation_size(indexrelid) as raw_size, idx_scan as index_scans FROM pg_stat_user_indexes JOIN pg_index USING(indexrelid) WHERE idx_scan = 0 AND idx_tup_read = 0 AND idx_tup_fetch = 0 AND NOT indisprimary AND NOT indisunique AND NOT indisexclusion AND indisvalid AND indisready AND pg_relation_size(indexrelid) > 8192 ORDER BY 4 DESC"

        if self.html_format:
            cmd = "psql %s --html -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)
        else:
            cmd = "psql %s -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get unused indexes: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if self.html_format:
            self.appendreport("<H4>Unused indexes are identified where there are no index scans and the size of the index is > 8 KB.</H4>\n")
        else:    
            self.appendreport("\nUnused indexes are identified where there are no index scans and the size of the index  is > 8 KB.\n")

        # See if this cluster has dependent slaves and if so give information warning
        if self.slavecnt > 0:
            msg = "%d slave(s) are dependent on this cluster.  Make sure unused indexes are also unused on the slave(s) before considering them as index drop candidates.\n" % self.slavecnt
            print (msg)
            if self.html_format:
                msg = "<H4><p style=\"color:red;\">" + msg + "</p><H4>"
                self.appendreport(msg)
            else:
                self.appendreport(msg+"\n")

        f = open(self.tempfile, "r")
        lineno = 0
        count  = 0
        for line in f:
            lineno = lineno + 1
            if self.verbose:
                print ("%d line=%s" % (lineno,line))
            aline = line.strip()
            if len(aline) < 1:
                continue
            elif '(0 rows)' in aline:
                continue

            if lineno == 1:
                if self.html_format:
                    self.appendreport(aline)
                else:
                    self.appendreport("          %s\n" % aline)
            else:
                if self.html_format:
                    self.appendreport(aline)
                else:
                    self.appendreport("%s\n" % aline)
        if self.html_format:
            self.appendreport("<p><br></p>")
        f.close()
        return SUCCESS, ""

    ###########################################################
    def do_report_tablemaintenance(self):

        if self.freezecandidates == True:
            sql = "WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select s.setting as autovac_freeze_max_age, n.nspname as schema, c.relname as table, age(c.relfrozenxid) as xid_age, pg_size_pretty(pg_table_size(c.oid)) as table_size, round((age(c.relfrozenxid)::float / s.setting::float) * 100) as pct from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r' and pg_table_size(c.oid) > 1073741824 and round((age(c.relfrozenxid)::float / s.setting::float) * 100) > 50 ORDER BY age(c.relfrozenxid)"
            if self.html_format:
                cmd = "psql %s --html -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)
            else:
                cmd = "psql %s -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get user table stats: %d %s\ncmd=%s\n" % (rc, results, cmd)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors

            if self.html_format:
                self.appendreport("<H4>List of tables that are past the midway point of going into transaction wraparound mode and therefore candidates for manual vacuum freeze.</H4>")
            else:
                self.appendreport("\nList of tables that are past the midway point of going into transaction wraparound mode and therefore candidates for manual vacuum freeze.\n")

            f = open(self.tempfile, "r")
            lineno = 0
            count  = 0
            for line in f:
                lineno = lineno + 1
                if self.verbose:
                    print ("%d line=%s" % (lineno,line))
                aline = line.strip()
                if len(aline) < 1:
                    continue
                elif '(0 rows)' in aline:
                    continue
                elif '(0 rows)' in aline:
                    continue

                if lineno == 1:
                    if self.html_format:
                        self.appendreport(aline)
                    else:     
                        self.appendreport("%s\n" % aline)
                else:
                    if self.html_format:
                         self.appendreport(aline)
                    else:
                        self.appendreport("%s\n" % aline)
            f.close()
            if self.html_format:
                self.appendreport("<p><br></p>")

        print ("")

        if self.analyzecandidates == False:
            return SUCCESS, ""

        sql = "select n.nspname || '.' || c.relname as table, last_analyze, last_autoanalyze, last_vacuum, last_autovacuum, u.n_live_tup::bigint, c.reltuples::bigint, round((u.n_live_tup::float / CASE WHEN c.reltuples = 0 THEN 1.0 ELSE c.reltuples::float  END) * 100) as pct from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = t.schemaname and t.tablename = c.relname and t.schemaname = u.schemaname and t.tablename = u.relname and n.nspname not in ('information_schema','pg_catalog') and (((c.reltuples > 0 and round((u.n_live_tup::float / c.reltuples::float) * 100) < 50)) OR ((last_vacuum is null and last_autovacuum is null and last_analyze is null and last_autoanalyze is null ) or (now()::date  - last_vacuum::date > 60 AND now()::date - last_autovacuum::date > 60 AND now()::date  - last_analyze::date > 60 AND now()::date  - last_autoanalyze::date > 60))) order by n.nspname, c.relname"

        if self.html_format:
            cmd = "psql %s --html -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)
        else:
            cmd = "psql %s -c \"%s\" > %s" % (self.connstring, sql, self.tempfile)
            
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get user table stats: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if self.html_format:
            self.appendreport("<H4>List of tables that have not been analyzed or vacuumed (manual and auto) in the last 60 days or whose size has changed significantly (n_live_tup/reltuples * 100 < 50) and therefore candidates for manual vacuum analyze.</H4>")
        else:    
            self.appendreport("\nList of tables that have not been analyzed or vacuumed (manual and auto) in the last 60 days or whose size has changed significantly (n_live_tup/reltuples * 100 < 50) and therefore candidates for manual vacuum analyze.\n")

        f = open(self.tempfile, "r")
        lineno = 0
        count  = 0
        for line in f:
            lineno = lineno + 1
            if self.verbose:
                print ("%d line=%s" % (lineno,line))
            aline = line.strip()
            if len(aline) < 1:
                continue
            elif '(0 rows)' in aline:
                continue

            if lineno == 1:
                if self.html_format:
                    self.appendreport(aline)
                else:
                    self.appendreport("              %s\n" % aline)
            else:
                if self.html_format:
                    self.appendreport(aline)
                else:
                    self.appendreport("%s\n" % aline)

        if self.html_format:
            self.appendreport("<p><br></p>")
        f.close()

        return SUCCESS, ""

    #############################################################################################
    def do_report_healthchecks(self):

        # setup special table format
        if self.html_format:
            html = "<table class=\"table1\" style=\"width:100%\"> <caption><h3>Health Checks</h3></caption>" + \
                   "<tr> <th>Status</th> <th>Category</th> <th>Analysis</th> </tr>"
            self.appendreport(html)

        '''
        slavecnt = len(self.slaves)
        # still need to check for empty string as first slot
        if slavecnt == 1 and  self.slaves[0].strip() == '':
            slavecnt = 0

        if self.verbose:
            print ("slavecnt=%d  slaves=%s" % (slavecnt, self.slaves))
        msg = "%d slave(s) were detected." % slavecnt
        print (msg)
        if self.html_format:
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Number of Slaves</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
            self.appendreport(html)
        '''

        #####################################
        # analyze pg major and minor versions
        #####################################
        if self.pgversionmajor < Decimal('9.6'):
            marker = MARK_WARN
            msg = "Unsupported major version detected: %.1f.  Please upgrade ASAP." % self.pgversionmajor
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">PG Major Version Summary</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"            
        elif self.pgversionmajor < Decimal('13.0'):
            marker = MARK_WARN        
            msg = "Current PG major version is not the latest (%.1f).  Consider upgrading to 13." % self.pgversionmajor
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">PG Major Version Summary</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                        
        else:
            marker = MARK_OK        
            msg = "Current PG major version is the latest (%.1f).  No major upgrade necessary." % self.pgversionmajor        
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">PG Major Version Summary</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"        

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg)
        print (marker+msg)        
        
        # latest versions: 13.1, 12.5, 11.10, 10.15, 9.6.20
        if self.pgversionmajor > Decimal('9.5'):
            if self.datediff.days > 120:
                # probably a newer minor version is already out since these minor versions were last updated in the program
                marker = MARK_WARN
	        msg = "Current version: %s.  Please upgrade to latest minor version." % self.pgversionminor
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">PG Minor Version Summary</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                            
            elif self.pgversionmajor == Decimal('9.6') and self.pgversionminor < '9.6.20':
                marker = MARK_WARN
                msg = "Current version: %s.  Please upgrade to latest minor version, 9.6.20." % self.pgversionminor
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">PG Minor Version Summary</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                            
            elif self.pgversionmajor == Decimal('10.0') and self.pgversionminor < '10.15':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to latest minor version, 10.15." % self.pgversionminor
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">PG Minor Version Summary</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                            
            elif self.pgversionmajor == Decimal('11.0') and self.pgversionminor < '11.10':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to latest minor version, 11.10." % self.pgversionminor
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">PG Minor Version Summary</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                            
            elif self.pgversionmajor == Decimal('12.0') and self.pgversionminor < '12.5':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to latest minor version, 12.5." % self.pgversionminor
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">PG Minor Version Summary</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                            
            elif self.pgversionmajor == Decimal('13.0') and self.pgversionminor < '13.1':
                marker = MARK_WARN        
                msg = "Current version: %s.  Please upgrade to latest minor version, 13.1." % self.pgversionminor
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">PG Minor Version Summary</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                            
            else:
                marker = MARK_OK        
                msg = "Current PG minor version is the latest (%s). No minor upgrade necessary." % self.pgversionminor        
                html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">PG Minor Version Summary</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"        

            if self.html_format:
                self.appendreport(html)
            else:
                self.appendreport(marker+msg)
            print (marker+msg)                

        #####################
        # get cache hit ratio
        #####################
        # SELECT datname, blks_read, blks_hit, round((blks_hit::float/(blks_read+blks_hit+1)*100)::numeric, 2) as cachehitratio FROM pg_stat_database ORDER BY datname, cachehitratio
        sql = "SELECT blks_read, blks_hit, round((blks_hit::float/(blks_read+blks_hit+1)*100)::numeric, 2) as cachehitratio FROM pg_stat_database where datname = '%s' ORDER BY datname, cachehitratio" % self.database
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get database cache hit ratio: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors
        cols = results.split('|')
        blks_read   = int(cols[0].strip())
        blks_hit    = int(cols[1].strip())
        cache_ratio = Decimal(cols[2].strip())
        if cache_ratio < Decimal('70.0'):
            marker = MARK_WARN
            msg = "low cache hit ratio: %.2f (blocks hit vs blocks read)" % cache_ratio
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10004;</font></td><td width=\"20%\"><font color=\"red\">Cache Hit Ratio</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        elif cache_ratio < Decimal('90.0'):
            marker = MARK_WARN        
            msg = "Moderate cache hit ratio: %.2f (blocks hit vs blocks read)" % cache_ratio
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"red\">Cache Hit Ratio</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_OK
            msg = "High cache hit ratio: %.2f (blocks hit vs blocks read)" % cache_ratio
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Cache Hit Ratio</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")
        print (marker+msg)

        ##########################
        # shared_preload_libraries
        ##########################
        if 'pg_stat_statements' not in self.shared_preload_libraries:
            marker = MARK_WARN
            msg = "pg_stat_statements extension not loaded."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"red\">Shared Preload Libraries</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_OK
            msg = "pg_stat_statements loaded"
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Shared Preload Libraries</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"        
        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")
        print (marker+msg)



        ######################################################
        # get connection counts and compare to max connections
        ######################################################
        sql = "select count(*) from pg_stat_activity"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get count of current connections: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors
        conns = int(results)
        result = float(conns) / self.max_connections
        percentconns = int(math.floor(result * 100))
        if self.verbose:
            print ("Max connections = %d   Current connections = %d   PctConnections = %d" % (self.max_connections, conns, percentconns))

        if percentconns > 80:
            # 80 percent is the hard coded threshold
            marker = MARK_WARN        
            msg = "Current connections (%d) are greater than 80%% of max connections (%d) " % (conns, self.max_connections)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Connections</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_OK
            msg = "Current connections (%d) are not too close to max connections (%d) " % (conns, self.max_connections)
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Connections</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")            
        print (marker+msg)


        #######################################################################
        # get existing "idle in transaction" connections longer than 10 minutes
        #######################################################################
        # NOTE: 9.1 uses procpid, current_query, and no state column, but 9.2+ uses pid, query and state columns respectively.  Also idle is <IDLE> in current_query for 9.1 and less
        #       <IDLE> in transaction for 9.1 but idle in transaction for state column in 9.2+
        if self.pgversionmajor < Decimal('9.2'):
            # select substring(current_query,1,50), round(EXTRACT(EPOCH FROM (now() - query_start))), now(), query_start  from pg_stat_activity;
            sql = "select count(*) from pg_stat_activity where current_query ilike \'<IDLE> in transaction%\' and round(EXTRACT(EPOCH FROM (now() - query_start))) > 10"
        else:
            # select substring(query,1,50), round(EXTRACT(EPOCH FROM (now() - query_start))), now(), query_start, state  from pg_stat_activity;
            sql = "select count(*) from pg_stat_activity where state = \'idle in transaction\' and round(EXTRACT(EPOCH FROM (now() - query_start))) > 10"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get count of idle in transaction connections: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors
        idle_in_transaction_cnt = int(results)

        if idle_in_transaction_cnt == 0:
            marker = MARK_OK
            msg = "No \"idle in transaction\" longer than 10 minutes were detected."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Idle In Transaction</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_WARN
            msg = "%d \"idle in transaction\" longer than 15 minutes were detected." % idle_in_transaction_cnt
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Idle In Transaction</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")
        print (marker+msg)


        ######################################
        # Get long running queries > 5 minutes
        ######################################
        # NOTE: 9.1 uses procpid, current_query, and no state column, but 9.2+ uses pid, query and state columns respectively.  Also idle is <IDLE> in current_query for 9.1 and less
        #       <IDLE> in transaction for 9.1 but idle in transaction for state column in 9.2+
        if self.pgversionmajor < Decimal('9.2'):
            # select procpid,datname,usename, client_addr, now(), query_start, substring(current_query,1,100), now() - query_start as duration from pg_stat_activity where current_query not ilike '<IDLE%' and current_query <> ''::text and now() - query_start > interval '5 minutes';
            sql = "select count(*) from pg_stat_activity where current_query not ilike '<IDLE%' and current_query <> ''::text and now() - query_start > interval '5 minutes'"
        else:
            # select pid,datname,usename, client_addr, now(), state, query_start, substring(query,1,100), now() - query_start as duration from pg_stat_activity where state not ilike 'idle%' and query <> ''::text and now() - query_start > interval '5 minutes';
            sql = "select count(*) from pg_stat_activity where state not ilike 'idle%' and query <> ''::text and now() - query_start > interval '5 minutes'"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get count of long running queries: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors
        long_queries_cnt = int(results)

        if long_queries_cnt == 0:
            marker = MARK_OK
            msg = "No \"long running queries\" longer than 5 minutes were detected."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Long Running Queries</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_WARN
            msg = "%d \"long running queries\" longer than 5 minutes were detected." % long_queries_cnt
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Long Running Queries</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")
        print (marker+msg)


        ##########################################################
        # Get lock waiting transactions where wait is > 30 seconds
        ##########################################################
        if self.pgversionmajor < Decimal('9.2'):
          # select procpid, datname, usename, client_addr, now(), query_start, substring(current_query,1,100), now() - query_start as duration from pg_stat_activity where waiting is true and now() - query_start > interval '30 seconds';
            sql = "select count(*) from pg_stat_activity where waiting is true and now() - query_start > interval '30 seconds'"
        elif self.pgversionmajor < Decimal('9.6'):
          # select pid, datname, usename, client_addr, now(), query_start, substring(query,1,100), now() - query_start as duration from pg_stat_activity where waiting is true and now() - query_start > interval '30 seconds';
            sql = "select count(*) from pg_stat_activity where waiting is true and now() - query_start > interval '30 seconds'"
        else:
            # new wait_event column replaces waiting in 9.6/10
            sql = "select count(*) from pg_stat_activity where wait_event is NOT NULL and state = 'active' and now() - query_start > interval '30 seconds'"

        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get count of blocked queries: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors
        blocked_queries_cnt = int(results)
        if blocked_queries_cnt == 0:
            marker = MARK_OK
            msg = "No \"Waiting/Blocked queries\" longer than 30 seconds were detected."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Waiting/Blocked queries</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_WARN
            msg = "%d \"Waiting/Blocked queries\" longer than 30 seconds were detected." % blocked_queries_cnt
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Waiting/Blocked queries</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")
        print (marker+msg)

        #################################
        # get archiving info if available
        #################################
        rc, results = self.get_readycnt()
        if rc != SUCCESS:
            errors = "Unable to get archiving status: %d %s\n" % (rc, results)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        readycnt = int(results)

        if self.verbose:
            print ("Ready Count = %d" % readycnt)

        if readycnt > 1000:
            if self.html_format:
                marker = MARK_WARN
                msg = "Archiving is behind more than 1000 WAL files. Current count: %d" % readycnt
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Archiving Status</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"

        elif readycnt == 0:
            if self.archive_mode == 'on':
                marker = MARK_OK
                msg = "Archiving is on and no WAL backup detected."            
                if self.html_format:
                    html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Archiving Status</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
                else:
                    msg = "Archiving Status: %s" % msg 
            else:
                marker = MARK_OK
                msg = "Archiving is off so nothing to analyze."
                if self.html_format:
                    html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Archiving Status</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
                else:
                    msg = "Archiving Status: %s" % msg 
                    
        else:
            marker = MARK_OK
            msg = "Archiving is working and not too far behind. WALs waiting to be archived=%d" % readycnt
            if self.html_format:
                html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Archiving Status</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
            else:
                msg = "Archiving Status: %s" % msg 

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")
        print (marker+msg)

        ###########################################################################################################################################
        # database conflicts: only applies to PG versions greater or equal to 9.1.  9.2 has additional fields of interest: deadlocks and temp_files
        ###########################################################################################################################################
        if self.pgversionmajor < Decimal('9.1'):
            msg = "No database conflicts found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Database Conflicts</font></td><td width=\"75%\"><font color=\"blue\">N/A</font></td></tr>"
            return SUCCESS, ""
            print (msg)
            if self.html_format:
                self.appendreport(html)
            return SUCCESS, ""

        if self.pgversionmajor < Decimal('9.2'):
            sql="select datname, conflicts from pg_stat_database where datname = '%s'" % self.database
        else:
            sql="select datname, conflicts, deadlocks, temp_files, temp_bytes from pg_stat_database where datname = '%s'" % self.database
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get database conflicts: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        cols = results.split('|')
        database   = cols[0].strip()
        conflicts  = int(cols[1].strip())
        deadlocks  = -1
        temp_files = -1
        temp_bytes = -1
        if len(cols) > 2:
            deadlocks  = int(cols[2].strip())
            temp_files = int(cols[3].strip())
            temp_bytes = int(cols[4].strip())            

        #  if self.verbose:
        #      print
        if conflicts > 0 or deadlocks > 0 or temp_files > 0:
            marker = MARK_WARN
            msg = "Database conflicts found: database=%s  conflicts=%d  deadlocks=%d  temp_files=%d  temp_bytes=%d" % (database, conflicts, deadlocks, temp_files, temp_bytes)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Database Conflicts (deadlocks, Query disk spillover, Standby cancelled queries)</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_OK
            msg = "No database conflicts found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Database Conflicts (deadlocks, Query disk spillover, Standby cancelled queries</font></td><td width=\"75%\"><font color=\"blue\">No database conflicts found.</font></td></tr>"

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")
        print (marker+msg)

        ###############################################################################################################
        # Check for checkpoint frequency
        # NOTE: Checkpoints should happen every few minutes, not less than 5 minutes and not more than 15-30 minutes
        #       unless recovery time is not a priority and High I/O SQL workload is in which case 1 hour is reasonable.
        ###############################################################################################################
        sql = "SELECT total_checkpoints, seconds_since_start / total_checkpoints / 60 AS minutes_between_checkpoints, checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time FROM (SELECT EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) AS seconds_since_start, (checkpoints_timed+checkpoints_req) AS total_checkpoints, checkpoints_timed, checkpoints_req, checkpoint_write_time / 1000 as checkpoint_write_time, checkpoint_sync_time / 1000 as checkpoint_sync_time FROM pg_stat_bgwriter) AS sub"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get checkpoint frequency: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        cols = results.split('|')
        total_checkpoints     = int(cols[0].strip())
        minutes               = Decimal(cols[1].strip())
        checkpoints_timed     = int(cols[2].strip())
        checkpoints_req       = int(cols[3].strip())
        checkpoint_write_time = int(float(cols[4].strip()))
        checkpoint_sync_time  = int(float(cols[5].strip()))        \
        # calculate average checkpoint time
        avg_checkpoint_seconds = ((checkpoint_write_time + checkpoint_sync_time) / (checkpoints_timed + checkpoints_req))

        if minutes < Decimal('5.0'):
            marker = MARK_WARN
            msg = "Checkpoints are occurring too fast, every %.2f minutes, and taking about %d minutes on average." % (minutes, (avg_checkpoint_seconds / 60))
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Checkpoint Frequency</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
        elif minutes > Decimal('60.0'):
            marker = MARK_WARN
            msg = "Checkpoints are occurring too infrequently, every %.2f minutes, and taking about %d minutes on average." % (minutes, (avg_checkpoint_seconds / 60))
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Checkpoint Frequency</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                  
        else:
            marker = MARK_OK
            msg = "Checkpoints are occurring every %.2f minutes, and taking about %d minutes on average." % (minutes, (avg_checkpoint_seconds / 60))
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Checkpoint Frequency</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")
        print (marker+msg)

        ####################################
        # Check some postgresql config parms
        ####################################
        sql = "with summary as (select name, setting from pg_settings where name in ('autovacuum', 'checkpoint_completion_target', 'data_checksums', 'idle_in_transaction_session_timeout', 'log_checkpoints', 'log_lock_waits',  'log_min_duration_statement', 'log_temp_files', 'shared_preload_libraries', 'track_activity_query_size') order by 1 ) select setting from summary order by name"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)        
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get configuration parameters: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors        
        # since we have multiple rows, we split based on carriage return, not pipe when one row is returned
        cols = results.split('\n')

        autovacuum                           = cols[0].strip()
        checkpoint_completion_target         = Decimal(cols[1].strip())
        data_checksums                       = cols[2].strip()
        idle_in_transaction_session_timeout = int(cols[3].strip())
        log_checkpoints                     = cols[4].strip()
        log_lock_waits                      = cols[5].strip()
        log_min_duration_statement          = int(cols[6].strip())
        log_temp_files                      = cols[7].strip()
        shared_preload_libraries            = cols[8].strip()
        track_activity_query_size           = int(cols[9].strip())

        #print ("autovac=%s  chk_target=%s  sums=%s  idle=%s  log_checkpoints=%s  log_locks= %s  log_min=%s  log_temp=%s  shared=%s  track=%s" \
        #      % (autovacuum, checkpoint_completion_target, data_checksums, idle_in_transaction_session_timeout, log_checkpoints, log_lock_waits, 
        #      log_min_duration_statement, log_temp_files, shared_preload_libraries, track_activity_query_size))

        msg = ''              
        if autovacuum != 'on':
            marker = MARK_WARN
            msg = "autovacuum is off.  "
        if checkpoint_completion_target <= 0.6:
            marker = MARK_WARN
            msg+= "checkpoint_completion_target is less than optimal.  "            
        if data_checksums != 'on':
            marker = MARK_WARN
            msg+= "data checksums is off.  "
        if idle_in_transaction_session_timeout == '0':
            marker = MARK_WARN
            msg+= "idle_in_transaction_session_timeout is turned off.  "
        if log_checkpoints != 'on':
            marker = MARK_WARN
            msg+= "log_checkpoints is off.  "
        if log_lock_waits != 'on':
            marker = MARK_WARN
            msg+= "log_lock_waits is off.  "            
        if log_min_duration_statement == '-1':
            marker = MARK_WARN
            msg+= "log_min_duration_statement is off.  "
        if log_temp_files == '-1':
            marker = MARK_WARN
            msg+= "log_temp_files is off.  "             
        if 'pg_stat_statements' not in shared_preload_libraries:
            marker = MARK_WARN
            msg+= "pg_stat_statements extension is not loaded.  "
        if track_activity_query_size < 8192:
            marker = MARK_WARN
            msg+= "track_activity_query_size may need to be increased or log queries may be truncated.  " 

        if msg != '':
            marker = MARK_WARN        
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Configuration Settings</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>" 
        else:
            marker = MARK_OK
            msg = "No configuration problems detected."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Configuration Settings</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"                         

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")

        print (marker+msg)


        
        ############################################################
        # Check checkpoints, background writers, and backend writers
        ############################################################
        # v2.1 fix: divident could be zero and cause division by zero error, so check first.
        sql = "select buffers_checkpoint + buffers_checkpoint + buffers_clean + buffers_backend as buffers from pg_stat_bgwriter"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get background/backend buffers count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors        

        if int(results) == 0:
            marker = MARK_WARN
            msg = "No buffers to check for checkpoint, background, or backend writers."
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10004;</font></td><td width=\"20%\"><font color=\"red\">Checkpoint/Background/Backend Writers</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"
            if self.html_format:
                self.appendreport(html)
            else:
                self.appendreport(marker+msg+"\n")
            print (marker+msg)
        else:            
            sql = "select checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_clean, maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc, checkpoint_write_time / 1000 as checkpoint_write_time, checkpoint_sync_time / 1000 as checkpoint_sync_time, (100 * checkpoints_req) / (checkpoints_timed + checkpoints_req) AS checkpoints_req_pct,    pg_size_pretty(buffers_checkpoint * block_size / (checkpoints_timed + checkpoints_req)) AS avg_checkpoint_write,  pg_size_pretty(block_size * (buffers_checkpoint + buffers_clean + buffers_backend)) AS total_written,  100 * buffers_checkpoint / (buffers_checkpoint + buffers_clean + buffers_backend) AS checkpoint_write_pct,    100 * buffers_clean / (buffers_checkpoint + buffers_clean + buffers_backend) AS background_write_pct, 100 * buffers_backend / (buffers_checkpoint + buffers_clean + buffers_backend) AS backend_write_pct from pg_stat_bgwriter, (SELECT cast(current_setting('block_size') AS integer) AS block_size) bs"

            cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get background/backend writers: %d %s\nsql=%s\n" % (rc, results, sql)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            cols = results.split('|')
            checkpoints_timed     = int(cols[0].strip())
            checkpoints_req       = int(cols[1].strip())
            buffers_checkpoint    = int(cols[2].strip())
            buffers_clean         = int(cols[3].strip())
            maxwritten_clean      = int(cols[4].strip())
            buffers_backend       = int(cols[5].strip())
            buffers_backend_fsync = int(cols[6].strip())
            buffers_alloc         = int(cols[7].strip())
            checkpoint_write_time = int(float(cols[8].strip()))
            checkpoint_sync_time  = int(float(cols[9].strip()))
            checkpoints_req_pct   = int(cols[10].strip())
            avg_checkpoint_write  = cols[11].strip()
            total_written         = cols[12].strip()
            checkpoint_write_pct  = int(cols[13].strip())
            background_write_pct  = int(cols[14].strip())
            backend_write_pct     = int(cols[15].strip())

            # calculate average checkpoint time
            avg_checkpoint_seconds = ((checkpoint_write_time + checkpoint_sync_time) / (checkpoints_timed + checkpoints_req))

            if self.verbose:
                msg = "chkpt_time=%d chkpt_req=%d  buff_chkpt=%d  buff_clean=%d  maxwritten_clean=%d  buff_backend=%d  buff_backend_fsync=%d  buff_alloc=%d, chkpt_req_pct=%d avg_chkpnt_write=%s total_written=%s chkpnt_write_pct=%d background_write_pct=%d  backend_write_pct=%d avg_checkpoint_time=%d seconds" \
                % (checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_clean, maxwritten_clean, buffers_backend, buffers_backend_fsync, buffers_alloc, checkpoints_req_pct, avg_checkpoint_write, total_written, checkpoint_write_pct, background_write_pct, backend_write_pct, avg_checkpoint_seconds)
                print (msg)

            msg = ''
            if buffers_backend_fsync > 0:
                marker = MARK_WARN
                msg = "bgwriter fsync request queue is full. Backend using fsync.  "
            if backend_write_pct > (checkpoint_write_pct + background_write_pct):
                marker = MARK_WARN
                msg += "backend writer doing most of the work.  Consider decreasing \"bgwriter_delay\" by 50% or more to make background writer do more of the work.  "
            if maxwritten_clean > 500000:
                # for now just use a hard coded value of 500K til we understand the math about this better
                marker = MARK_WARN
                msg += "background writer stopped cleaning scan %d times because it had written too many buffers.  Consider increasing \"bgwriter_lru_maxpages\".  " % maxwritten_clean
            if checkpoints_req > checkpoints_timed:
                marker = MARK_WARN
                msg += "\"checkpoints requested\" contributing to a lot more checkpoints (%d) than \"checkpoint timeout\" (%d).  Consider increasing \"checkpoint_segments or max_wal_size\".  " % (checkpoints_req, checkpoints_timed)            
            if buffers_backend_fsync > 0:
                marker = MARK_WARN
                msg += "storage problem since fsync queue is completely filled. buffers_backend_fsync = %d." % (buffers_backend_fsync)            
            if buffers_clean > buffers_backend:
                marker = MARK_WARN
                msg += "backends doing most of the cleaning. Consider increasing bgwriter_lru_multiplier and decreasing bgwriter_delay.  It could also be a problem with shared_buffers not being big enough."                        
           
            if msg != '':
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Checkpoint/Background/Backend Writers</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>" 
            else:
                marker = MARK_OK
                msg = "No problems detected with checkpoint, background, or backend writers."
                html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Checkpoint/Background/Backend Writers</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"

            if self.html_format:
                self.appendreport(html)
            else:
                self.appendreport(marker+msg+"\n")
            print (marker+msg)


        ########################
        # orphaned large objects
        ########################

        if self.in_recovery:
            #NOTE: cannot run this against slaves since vacuumlo will attempt to create temp table
            numobjects = "-1"
        else:
            # v1.2 fix: always use provided port number
            if self.dbuser == '':
                user_clause = "-p %s" % (self.dbport)
            elif self.dbhost == '':
                user_clause = " %s -U %s -p %s" % (self.dbuser, self.dbport)
            else:
                user_clause = " -h %s -U %s -p %s" % (self.dbhost, self.dbuser, self.dbport)

            cmd = "%s/vacuumlo -n %s %s" % (self.pgbindir, user_clause, self.database)
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get orphaned large objects: %d %s\ncmd=%s\n" % (rc, results, cmd)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors

            # expecting substring like this --> "Would remove 35 large objects from database "agmednet.core.image"."
            numobjects = (results.split("Would remove"))[1].split("large objects")[0]

        if int(numobjects) == -1:
            marker = MARK_OK
            msg = "N/A: Unable to detect orphaned large objects on slaves."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Orphaned Large Objects</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"             
        elif int(numobjects) == 0:
            marker = MARK_OK
            msg = "No orphaned large objects were found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Orphaned Large Objects</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_WARN
            msg = "%d orphaned large objects were found.  Consider running vacuumlo to remove them." % int(numobjects)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Orphaned Large Objects</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")
        print (marker+msg)

        ##################################
        # Check for bloated tables/indexes
        ##################################
        sql = "SELECT count(*) FROM (SELECT  schemaname, tablename, cc.reltuples, cc.relpages, bs,  CEIL((cc.reltuples*((datahdr+ma- (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,  COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages, COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta FROM ( SELECT   ma,bs,schemaname,tablename,   (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,   (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2 FROM ( SELECT schemaname, tablename, hdr, ma, bs, SUM((1-null_frac)*avg_width) AS datawidth, MAX(null_frac) AS maxfracsum,  hdr+( SELECT 1+COUNT(*)/8 FROM pg_stats s2 WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename ) AS nullhdr FROM pg_stats s, ( SELECT (SELECT current_setting('block_size')::NUMERIC) AS bs, CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr, CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma FROM (SELECT version() AS v) AS foo ) AS constants  GROUP BY 1,2,3,4,5 ) AS foo) AS rs  JOIN pg_class cc ON cc.relname = rs.tablename  JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema' LEFT JOIN pg_index i ON indrelid = cc.oid LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid ) AS sml where ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) > 20 OR ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) > 20 or CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END > 10737418240 OR CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END > 10737418240"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get table/index bloat count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if int(results) == 0:
            marker = MARK_OK
            self.bloatedtables = False
            msg = "No bloated tables/indexes were found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Bloated Tables and/or Indexes</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"      
        else:
            marker = MARK_WARN
            self.bloatedtables = True
            msg = "%d bloated tables/indexes were found (See output file for details)." % int(results)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Bloated Tables and/or Indexes</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"         

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")
        print (marker+msg)


        ##########################
        # Check for unused indexes
        ##########################
        sql="SELECT count(*) FROM pg_stat_user_indexes JOIN pg_index USING(indexrelid) WHERE idx_scan = 0 AND idx_tup_read = 0 AND idx_tup_fetch = 0 AND NOT indisprimary AND NOT indisunique AND NOT indisexclusion AND indisvalid AND indisready AND pg_relation_size(indexrelid) > 8192"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get unused indexes count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if int(results) == 0:
            marker = MARK_OK
            self.unusedindexes = False
            msg = "No unused indexes were found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Unused Indexes</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        else:
            marker = MARK_WARN
            self.unusedindexes = True
            msg = "%d unused indexes were found (See output file for details)." % int(results)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Unused Indexes</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                        

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")            
        print (marker+msg)
        
        ###################################
        # Check for short-lived connections
        ###################################
        sql="select cast(extract(epoch from avg(now()-backend_start)) as integer) as age from pg_stat_activity"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get average connection time: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        avgsecs = int(results)
        if avgsecs > 172800:
            # 24 hours, so warn to refresh connections
            marker = MARK_WARN
            msg = "Connections average more than 24 hours (%d minutes). Consider refreshing these connections 2-3 times per day." % (avgsecs / 60)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Connection Time</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                                    
        elif avgsecs >= 120:
            marker = MARK_OK
            msg = "Connections average more than 2 minutes (%d). This seems acceptable." % (avgsecs / 60)
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Connection Time</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"
        elif avgsecs < 200:
            marker = MARK_WARN
            msg = "Connections average less than 2 minutes (%d).  Use or tune a connection pooler to keep these connections alive longer." % (avgsecs / 60)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Connection Time</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                        

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")            
        print (marker+msg)        


        ####################################
        # Check for vacuum freeze candidates
        ####################################
        sql="WITH settings AS (select s.setting from pg_settings s where s.name = 'autovacuum_freeze_max_age') select count(c.*) from settings s, pg_class c, pg_namespace n WHERE n.oid = c.relnamespace and c.relkind = 'r' and pg_table_size(c.oid) > 1073741824 and round((age(c.relfrozenxid)::float / s.setting::float) * 100) > 50"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get vacuum freeze candidate count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if int(results) == 0:
            marker = MARK_OK
            self.freezecandidates = False
            msg = "No vacuum freeze candidates were found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Vacuum Freeze Candidates</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"           
        else:
            marker = MARK_WARN
            self.freezecandidates = True
            msg = "%d vacuum freeze candidates were found (See output file for details)." % int(results)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Vacuum Freeze Candidates</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"              

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")            
        print (marker+msg)


        ##############################
        # Check for analyze candidates
        ##############################
        sql="select count(*) from pg_namespace n, pg_class c, pg_tables t, pg_stat_user_tables u where c.relnamespace = n.oid and n.nspname = t.schemaname and t.tablename = c.relname and t.schemaname = u.schemaname and t.tablename = u.relname and n.nspname not in ('information_schema','pg_catalog') and (((c.reltuples > 0 and round((u.n_live_tup::float / c.reltuples::float) * 100) < 50)) OR ((last_vacuum is null and last_autovacuum is null and last_analyze is null and last_autoanalyze is null ) or (now()::date  - last_vacuum::date > 60 AND now()::date - last_autovacuum::date > 60 AND now()::date  - last_analyze::date > 60 AND now()::date  - last_autoanalyze::date > 60)))"
        cmd = "psql %s -t -c \"%s\"" % (self.connstring, sql)
        rc, results = self.executecmd(cmd, False)
        if rc != SUCCESS:
            errors = "Unable to get vacuum analyze candidate count: %d %s\nsql=%s\n" % (rc, results, sql)
            aline = "%s" % (errors)
            self.writeout(aline)
            return rc, errors

        if int(results) == 0:
            marker = MARK_OK
            self.analyzecandidates = False
            msg = "No vacuum analyze candidates were found."
            html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Vacuum Analyze Candidates</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"          
        else:
            marker = MARK_WARN
            self.analyzecandidates = True
            msg = "%d vacuum analyze candidate(s) were found (See output file for details)." % int(results)
            html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Vacuum Analyze Candidates</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"             

        if self.html_format:
            self.appendreport(html)
        else:
            self.appendreport(marker+msg+"\n")            
        print (marker+msg)


        #############################################
        # kernel and network health checks begin here
        # ONLY applies to linux at current time and 
        # only if localhost or 127.0.0.1 provided
        #############################################

        if self.opsys == 'posix' and (self.dbhost == 'localhost' or self.dbhost == '127.0.0.1'):


            # Check for high number of standby connections, i.e., TIME_WAIT states
            # This usually indicates a lot of short-lived connections and the absence of a connection pooler
            '''
            Due to the way TCP/IP works, connections can not be closed immediately. Packets may arrive out of order or be retransmitted after the connection has been closed. CLOSE_WAIT indicates that the remote endpoint (other side of the connection) has closed the connection. TIME_WAIT indicates that local endpoint (this side) has closed the connection. The connection is being kept around so that any delayed packets can be matched to the connection and handled appropriately. The connections will be removed when they time out within four minutes.  Basically the "WAIT" states mean that one side closed the connection but the final confirmation of the close is pending.

            ss -an state time-wait | wc -l
            netstat -nat | egrep 'TIME_WAIT' | wc -l
            netstat -ntu  | grep TIME_WAIT | wc -l

            Now, let's see why this state can be annoying on a server handling a lot of connections. There are three aspects of the problem:
            the slot taken in the connection table preventing new connections of the same kind,
            the memory occupied by the socket structure in the kernel, and
            the additional CPU usage.
            A connection in the TIME-WAIT state is kept for one minute in the connection table. This means, another connection with the same quadruplet (source address, source port, destination address, destination port) cannot exist.
            The result of ss -tan state time-wait | wc -l is not a problem per se!
            '''

            cmd =  "ss -an state time-wait | wc -l"
            rc, results = self.executecmd(cmd, False)
            if rc != SUCCESS:
                errors = "Unable to get network standby connections count: %d %s\nsql=%s\n" % (rc, results, sql)
                aline = "%s" % (errors)
                self.writeout(aline)
                return rc, errors
            standby = int(results)
            if standby < 1000:
                marker = MARK_OK
                msg = "Network: Relatively few network standby connections (%d)." % standby
                html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Network Standby Connections</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"    
            else:
                marker = MARK_WARN  
                msg = "Network: High number of standby connections: %d.  This may indicate a lot of short-lived connections and the absence of a connection pooler." % standby
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Network Standby Connections</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"       

            if self.html_format:
                self.appendreport(html)
            else:
                self.appendreport(marker+msg+"\n")                
            print (marker+msg)

            msg = ''
            if self.overcommit_memory == 0:
                marker = MARK_WARN              
                msg = "Kernel memory overcommitment is currently allowed (default setting: 0).  The OOM Killer may kill at least one of the PostgreSQL processes, which may lead to data corruption."
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Kernel Memory Capacity</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                       
            else:
                marker = MARK_OK
                msg = "Kernel Memory Capacity: OOM Killer is disabled (%d)." % self.overcommit_memory
                html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Kernel Memory Capacity</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"                
            
            if self.html_format:
                self.appendreport(html)
            else:
                self.appendreport(marker+msg+"\n")                
            print (marker+msg)

            if self.overcommit_ratio <= 50:
                marker = MARK_WARN              
                msg = "Kernel memory overcommit ratio is too low (%d). Consider increasing to 70 or higher." % self.overcommit_ratio
                html = "<tr><td width=\"5%\"><font color=\"red\">&#10060;</font></td><td width=\"20%\"><font color=\"red\">Kernel Memory Capacity</font></td><td width=\"75%\"><font color=\"red\">" + msg + "</font></td></tr>"                       
            else:
                marker = MARK_OK
                msg = "Kernel Memory Capacity: overcommit ratio is OK (%d)." % self.overcommit_memory
                html = "<tr><td width=\"5%\"><font color=\"blue\">&#10004;</font></td><td width=\"20%\"><font color=\"blue\">Kernel Memory Capacity</font></td><td width=\"75%\"><font color=\"blue\">" + msg + "</font></td></tr>"                
            
            if self.html_format:
                self.appendreport(html)
            else:
                self.appendreport(marker+msg+"\n")                
            print (marker+msg)


        ########################################
        ### end of linux only network checks ###
        ########################################

        if self.html_format:
            # finish special table format
            self.appendreport("</table>")
            self.appendreport("<p><br></p>")


        return SUCCESS, ""


    ###########################################################
    def delay(self, freeze):
        return SUCCESS, ""
       

##### END OF CLASS DEFINITION

#############################################################################################
def setupOptionParser():
    parser = OptionParser(add_help_option=False, description=DESCRIPTION)
    parser.add_option("-h", "--dbhost",         dest="dbhost",   help="DB Host Name or IP",                     default="",metavar="DBHOST")
    parser.add_option("-p", "--port",           dest="dbport",   help="db host port",                           default="5432",metavar="DBPORT")
    parser.add_option("-U", "--dbuser",         dest="dbuser",   help="db host user",                           default="",metavar="DBUSER")
    parser.add_option("-d", "--database",       dest="database", help="database name",                          default="",metavar="DATABASE")
    parser.add_option("-n", "--schema",         dest="schema", help="schema name",                              default="",metavar="SCHEMA")
    parser.add_option("-m", "--html",           dest="html", help="html report format",                         default=False, action="store_true")
    parser.add_option("-r", "--dry_run",        dest="dry_run", help="Dry Run Only",                            default=False, action="store_true")
    parser.add_option("-v", "--verbose",        dest="verbose", help="Verbose Output",                          default=False, action="store_true")

    return parser

#############################################################################################

#################################################################
#################### MAIN ENTRY POINT ###########################
#############################################@###################

optionParser   = setupOptionParser()
(options,args) = optionParser.parse_args()

# load the instance
pg = maint()

# Load and validate parameters
rc, errors = pg.set_dbinfo(options.dbhost, options.dbport, options.dbuser, options.database, options.schema, \
                           options.html, options.dry_run, options.verbose, sys.argv)
if rc != SUCCESS:
    print (errors)
    optionParser.print_help()
    sys.exit(1)

print ("%s  version: %.1f  %s     Python Version: %d     PG Version: %s     PG Database: %s\n\n" % (PROGNAME, VERSION, ADATE, sys.version_info[0], pg.pgversionminor, pg.database))

#print ("globals=%s" % globals())
#print ("locals=%s" % locals())

rc, results = pg.do_report()
if rc < SUCCESS:
    pg.cleanup()
    sys.exit(1)

pg.cleanup()

sys.exit(0)

