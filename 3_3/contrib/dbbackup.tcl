#!/opt/tcl8.3.0/bin/tclsh8.3

#  .-------------.------------------------------------------------------.
#  |  module     |  dbbackup.tcl                                        |
#  `-------------^------------------------------------------------------'

#  .--------------------------------------------------------------------.
#  |  				Revisions				|
#  |--------------------------------------------------------------------|
#  |  07/25/96 (TMH) eliminated the need for a library file.		|
#  |  02/10/97 (TMH) converted to tcl7.6, oratcl 2.4.			|
#  |  12/02/98 (TMH) eliminate all pipes to /bin/sh for final release	| 
#  `--------------------------------------------------------------------'

#  .--------------------------------------------------------------------.
#  |            Copyright (c) 1998, Purdue University                   |
#  |                     All rights reserved.                           |
#  `--------------------------------------------------------------------'

#  .--------------------------------------------------------------------.
#  |  Redistribution and use in source and binary forms are permitted   |
#  |  provided that:                                                    |
#  |                                                                    |
#  | (1) source distributions retain this entire copyright notice and   |
#  |     comment, and                                                   |
#  | (2) distributions including binaries display the following         |
#  |      acknowledgement:                                              |
#  |                                                                    |
#  |   "This product includes software developed by Purdue University." |
#  | in the documentation or other materials provided with the          |
#  | distribution and in all advertising materials mentioning features  |
#  | or use of this software.                                           |
#  |                                                                    |
#  |   The name of the University may not be used to endorse or promote |
#  | products derived from this software without specific prior written |
#  | permission.                                                        |
#  |                                                                    |
#  | THIS SOFTWARE IS PROVIDED ``AS IS'' AND WITHOUT ANY EXPRESS OR     |
#  | IMPLIED WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED     |
#  | WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR      |
#  | PURPOSE.                                                           |
#  `--------------------------------------------------------------------'

package require -exact Oratcl 2.7

set uidpswd /
set df_dest /opt/oracle/backup
set cf_dest [file join $df_dest "ctl$env(ORACLE_SID).ctl"]

set cur {}
set lda {}


#  .--------------------------------------------------------------------.
#  |  sysdate								|
#  `--------------------------------------------------------------------'
proc sysdate {} {
	return [clock format [clock seconds] -format {%c}]
}


#  .-------------.------------------------------------------------------.
#  |  procedure  |  ora_Connect						|
#  |  date       |  (09/02/94)                                          |
#  `-------------^------------------------------------------------------'
proc ora_Connect {} {
	global uidpswd
	global cur
	global lda

	set retcode [catch {set lda [oralogon ${uidpswd}]}]
	if {$retcode == 0} {
		set cur [oraopen $lda]
	}
	return $retcode
}


#  .-------------.------------------------------------------------------.
#  |  procedure  |  ora_Disconnect					|
#  |  author     |  Todd M. Helfter                                     |
#  |  date       |  (09/02/94)                                          |
#  `-------------^------------------------------------------------------'
proc ora_Disconnect {} {
	global lda
	return [catch {oralogoff $lda }]
}


#  .-------------.------------------------------------------------------.
#  |  procedure  |  print_log						|
#  `-------------^------------------------------------------------------'
proc print_log {} {
	global cur oramsg

	set log_min {}
	set log_max {}
	set sql {select min(sequence#), max(sequence#) from v$log}
	if {[catch {orasql $cur $sql}] == 0} {
		orafetch $cur "set log_min @1; set log_max @2"
	}
	puts stdout [format "\nOldest online log sequence\t%s" $log_min]
	puts stdout [format "Current log sequence\t\t%s\n" $log_max]
	flush stdout
}


#  .-------------.------------------------------------------------------.
#  |  procedure  |  ora_SQL						|
#  `-------------^------------------------------------------------------'
proc ora_SQL {sql_str} {
	global cur oramsg

	set dbret [catch {orasql $cur $sql_str}]
	if {$dbret != 0} {
		puts stdout $oramsg(errortxt)
		flush stdout
	} else {
		orafetch $cur {
			puts stdout @0
			flush stdout
		}
	}
}


#  .--------------------------------------------------------------------.
#  |  query_info							|
#  `--------------------------------------------------------------------'
proc query_info {} {
	global cur ts_list df_list oramsg uidpswd

	set ts_list {}
	if {[ora_Connect] == 0} {
		set sql0 "SELECT tablespace_name FROM sys.dba_tablespaces"
		if {[catch {orasql $cur $sql0}] == 0} {
			orafetch $cur {lappend ts_list @0}
		}
	} else {
		puts stdout $oramsg(errortxt)
		flush stdout
		exit 1
	}

	foreach ts_name $ts_list {
		set df_list($ts_name) {}
		set sql1 "SELECT file_name FROM sys.dba_data_files \
			where tablespace_name = '$ts_name'"
		if {[catch {orasql $cur $sql1}] == 0} {
			orafetch $cur {lappend df_list($ts_name) @0}
		} else {
			puts stdout $oramsg(errortxt)
			flush stdout
			exit 1
		}
	}
}


#  .--------------------------------------------------------------------.
#  |  print_info							|
#  `--------------------------------------------------------------------'
proc print_info {} {
	global ts_list df_list

	foreach ts_name $ts_list {
		puts stdout "ts_name : $ts_name"
		flush stdout
		foreach df_name $df_list($ts_name) {
			puts stdout "  df_name : $df_name"
			flush stdout
		}
	}
}


#  .--------------------------------------------------------------------.
#  |  ora_Backup							|
#  `--------------------------------------------------------------------'
proc ora_Backup {} {
	global ts_list df_list df_dest cf_dest

	print_log

	foreach ts_name $ts_list {

		puts stdout "[sysdate] | begin online backup for : $ts_name"
		flush stdout
		ora_SQL "alter tablespace $ts_name begin backup"

		foreach df_name $df_list($ts_name) {
			puts stdout "[sysdate] | copying $df_name to $df_dest."
			flush stdout
			file copy -force -- $df_name $df_dest
		}
		puts stdout "[sysdate] | end online backup for : $ts_name"
		flush stdout
		ora_SQL "alter tablespace $ts_name end backup"
	}
	puts stdout "[sysdate] | switching logfile."
	flush stdout
	ora_SQL {alter system switch logfile}

	print_log

	puts stdout "[sysdate] | copying control file to $cf_dest."
	flush stdout
	ora_SQL "alter database backup controlfile to '$cf_dest' reuse"
}


#  .-------------.------------------------------------------------------.
#  |  procedure  |  main                                                |
#  |  purpose    |  scan database and report results to parent window  	|
#  |  author     |  Todd M. Helfter                                     |
#  |  date       |  (10/13/94)                                          |
#  `-------------^------------------------------------------------------'
proc main {} {
	global cur uidpswd oramsg ts_list df_list

	query_info
	print_info

	ora_Backup
	ora_Disconnect
}


main
exit 0
