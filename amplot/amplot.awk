#
# Amanda, The Advanced Maryland Automatic Network Disk Archiver
# Copyright (c) 1992-1998, 2000 University of Maryland at College Park
# All Rights Reserved.
#
# Permission to use, copy, modify, distribute, and sell this software and its
# documentation for any purpose is hereby granted without fee, provided that
# the above copyright notice appear in all copies and that both that
# copyright notice and this permission notice appear in supporting
# documentation, and that the name of U.M. not be used in advertising or
# publicity pertaining to distribution of the software without specific,
# written prior permission.  U.M. makes no representations about the
# suitability of this software for any purpose.  It is provided "as is"
# without express or implied warranty.
#
# U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
# BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
# OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
# CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
# Author: Olafur Gudumundsson, ogud@tis.com
# formerly at:     Systems Design and Analysis Group
#		   Computer Science Department
#		   University of Maryland at College Park
#
# An awk program to parse the amdump file and output the information
# in a form at the gnuplot program amplot.g wants
#
#	Creation Date: April 1992
#	modified: Aug 1993
#       Modified for Amanda-2.2: Dec 1993
#       Modified for Amanda-2.2: Mar 1994 and May 1994 and June 1994
#       Enhanced: April 1995
#	Input: One amdump file 
#	Output: Number of files that get fed into gnuplot
#
BEGIN{
# The folowing parameters may have to be set to suit each site, both 
# parameters are expressed in HOUR's. 
# If your average amanda dump is more than 3 hours you should increase the
# value of maxtime, similary if your dumps are finishing in less than 2 hours
# you should decrease the value of maxtime.
# This is now setable from amplot's command line.
#	maxtime  = 4;			# how long to plot graph for in hours

# Min host controls the reporting of hosts that take long in dumping
# This varible can be set explicity or as a fraction of maxtime
# If you are seeing too many hosts reported increase the value of this 
# constant
#
	min_host = maxtime * 0.75;	# good rule of thumb
#	min_host = 2.5;			# expicit cutoff value in hours

#
# DO NOT CHANGE ANYTHING BELOW THIS LINE
#
	time_scale = 60;	# display in minutes DO NOT CHANGE
	maxtime  *= time_scale; # convert to minutes
	min_host *= time_scale *time_scale; # convert to seconds
                                                # dumping than this 
        disk_raise = 120;	# scaling factors for Holding disk graph
        tape_raise = 90;
	dump_shift = 7.5;	# scaling factors for Dumpers idle graph
	dump_raise = 0;
	que_raise  = 300;	# scaling factors for the queue's
	count_scale= 1.0/3.0;	# new scale 
				# scaling factors for the x axis 
	bandw_raise = 250;
	bandw_scale = 30/300;   # default calculated below 

	holding_disk = -1;      # uninitialized
		
	cnt        = 0;		# default values for counters
	din 	   = 0;		# number of dumps to holding disk
	dout	   = 0; 	# number of dumps to tape 
	tapeq	   = 0;		# how many dumps in tape queue
	tape_err   = 0;		# how many tape errors
	tout	   = 0;		# data written out to tape 
	quit	   = 0;		# normal end of run
	plot_fmt   = "%7.2f %6.2f\n%7.2f %6.2f\n";  # format of files for gnuplot
	plot_fmt1  = "%7.2f %6.2f\n";  # format of files for gnuplot
}
	
{		# state machine for processing input lines lines
	if( $1 == "driver:") {
		if($2=="result")            do_result();
		else if( $2=="state")       do_state();
		else if( $2=="interface-state") ;
		else if( $2=="hdisk-state") do_hdisk++;
		else if( $2=="flush" && $3=="size" ) {
			flush_size = $4;
		}
		else if( $2=="start" && $3=="time")       do_start();
		else if( $2=="send-cmd") { 
			if( $7=="FILE-DUMP"){
			  file_dump++;
			  dmpr_strt[$6]=$4;
			  host[$6]=$10;
			  disk[$6]=$12;
			  level[$6]=$14;
			}
			else if( $7 == "FILE-WRITE") file_write++;
			else if( $7 == "START-TAPER") fil = $9;
		}
		else if( $2=="finished-cmd") cmd_fin++;
		else if( $2=="started")      forked++;
		else if( $2=="QUITTING")     do_quit();
		else if( $2=="find_diskspace:") ; #eat this line
		else if( $2=="assign_holdingdisk:") ; #eat this line
		else if( $2=="adjust_diskspace:") ; #eat this line
		else if( $2=="tape" && $3=="size") ; #eat this line
		else if( $2=="dump" && $3=="failed") ; #eat this line
		else if( $2=="taper" && $3=="failed") ; #eat this line
		else if( $2=="dumping" || $2 == "adding" || $2 == "holding-disks:") 
		  dumping++; # eat this line
		else if( $2!="FINISHED" && $2 != "pid" && $2 != "taper-tryagain"&& $2!="startaflush:")
		  print fil,"Unknown statement#",$0;
	}
	else if ( $1 == "planner:") {
		if( $2 == "SKIPPED" || $2 == "FAILED") {
			failed++;
			print fil, "INFO#", $0;
		}
	}
	else if( $1 == "GENERATING")        sched_start=NR;
	else if( $1 == "ENDFLUSH")          sched_start=NR;
	else if( $1 == "DELAYING")          do_moves();    # find estimated size
	else if( $1 == "dumper:") {
		if($4 != "starting" && $2 != "pid" && $2 != "stream_client:" && $2 != "dgram_bind:") 
	            print fil, "INFO#", $0;
	}
	else if( $1 == "taper:") {
		if($3 != "label" && $3 != "end" && $2 != "DONE" && $2 != "pid" && $2 != "slot" && $2 != "reader-side:" && $2 != "page" && $2 != "buffer" && $3 != "at" && $3 != "switching" && $2 != "slot:" && $2 != "status")
		    print fil, "INFO#", $0;
	}
	else if( $1 == "FLUSH") {
		no_flush++;
	}
	else if( NF==1 && sched_start > 0 && NR-sched_start > 1) { # new style end of schedule
		no_disks = NR-sched_start-2; # lets hope there are no extra lines
		sched_start = 0;
	}
}

function do_state(){		# state line is printed out after driver
				# finishes pondering new actions
				# it reports the state as seen be driver
# fields in the state line 
# $2 = "state" 		# $3 = "time" 		# $4 = time_val
# $5 = "free"		# $6 = "kps:"		# $7 =  free_kps
# $8 = "space:"		# $9 = space		# $10 = "taper:"
# $11 = "writing"/"idle"# $12 = "idle-dumpers:"
# $13 = #idle 		# $14 = "qlen"		# $15 = "tapeq:"
# $16 = #waiting	# $17 = "runq:"		# $18 = #not started 
# $19 = "roomq"		# $20 = #roomq		# $21 = "wakeup:"
# $22 = #wakeup		# $23 = "driver-idle:"	# $23 = status

	cnt++;					# number of event
	time = $4/time_scale;
	#Check overflow in driver ouput (big value instead of negative)
	if($7>0 && $7 < 0x7fffffff)
		unused = (bandw - $7)*bandw_scale+bandw_raise;
	else
		unused = bandw_raise;
	if( unused != unused_old) 
		printf plot_fmt, time, unused_old, time,unused >>"bandw_free";
	unused_old = unused;

	if(holding_disk_old != $9) {
		disk_alloc_time[disk_a] = time;
		disk_alloc_space[disk_a] = holding_disk_old;
		disk_a++;
		disk_alloc_time[disk_a] = time;
		disk_alloc_space[disk_a] = $9;
		disk_a++;
		holding_disk_old = $9;
	}

	twait = tsize;
	if(twait_old != twait) {
		twait_time[twait_a] = time;
		twait_wait[twait_a] = twait_old;
		twait_a++;
		twait_time[twait_a] = time;
		twait_wait[twait_a] = twait;
		twait_a++;
		twait_old = twait;
	}

	active = (dumpers-$13)*dump_shift+dump_raise;
	if( active != active_old )
		printf plot_fmt, time, active_old, time, active >> "dump_idle";
	active_old = active;

# tape on or off
	if($11=="writing")state = tape_raise+10;
	else              state = tape_raise;
	if( state != state_old )
		printf plot_fmt, time, state_old, time, state >> "tape_idle";
	state_old = state;

	run = $18*count_scale+que_raise;
	if( run != run_old )
		printf plot_fmt, time, run_old, time, run >> "run_queue";
	run_old = run;

	finish = written * count_scale+que_raise;
	if( finish != finish_old )
		printf plot_fmt, time, finish_old, time, finish >> "finished";
	finish_old = finish;

	tapeQ = $16 * count_scale+que_raise;
	if( tapeQ != tapeQ_old )
		printf plot_fmt, time, tapeQ_old, time, tapeQ >> "tape_queue";
	tapeQ_old = tapeQ;

}

function do_start() { 		# get configuration parameters
	dumpers    = $6;	# how many 
	day        = $14;
	dump_shift = 75/dumpers; 
	bandw      = $8;		
	bandw_scale = (30/bandw);
	unused_old = bandw_raise;
	print 0, unused_old > "bandw_free";
	if( sched_start >0 ) {
		no_disks = NR-sched_start-1; # backward compatability
		sched_start =0;
		print "do_start: no_disks", no_disks, $0;
	}
	no_disks += no_flush;
	size        = $10/1024;	       # size of holding disk in MB
	holding_disk= $10 + flush_size;
	init_holding_disk= $10 + flush_size;
	holding_disk_old = $10;
	disk_a = 0;
	disk_alloc_time[disk_a] = 0;
	disk_alloc_space[disk_a] = holding_disk_old;
	disk_a++;
	tsize = flush_size;
	twait_old = tsize;
	twait_a = 0;
	twait_time[twait_a] = 0;
	twait_wait[twait_a] = twait_old;
	twait_a++;
	if( NF==14) {		# original file was missing this
		policy="FIFO";
		alg   ="InOrder";
	}
	else if(NF>=18) {		# newer files have this format
		policy = $18;
		alg = $16;
		if( alg=="drain-ends") big = $20; 
	}
	
	start = $4;	# this is the start time of the first dump 
			# taper idle to this point should not be included
	run_old = no_disks*cont_scale+que_raise;
	print 0, run_old		>"run_queue";
	finish_old = tapeQ_old = que_raise;
	print 0, finish_old		>"finished"; 	
	print 0, tapeQ_old		>"tape_queue" ;	
	state_old = tape_raise;
	print 0, state_old 		> "tape_idle";
	active_old = dump_raise;
	print 0,active_old 		>"dump_idle";

}

function do_quit(){		# this is issued by driver at the end
				# when it has nothing more to do
	cnt++;
	quit = 1;
	tim  = $4 / time_scale;

	disk_alloc_time[disk_a] = tim;
	disk_alloc_space[disk_a] = holding_disk_old;
	disk_a++;
	max_space=disk_alloc_space[0];
	for(a=0; a<disk_a; a++) {
		if(disk_alloc_space[a] > max_space) {
			max_space = disk_alloc_space[a];
		}
	}

	space_change = 0;
	if(max_space > holding_disk) {
		space_change = max_space - holding_disk;
		holding_disk = max_space;
	}

	twait_time[twait_a] = tim;
	twait_wait[twait_a] = twait_old;
	twait_a++;
	min_wait=twait_wait[0];
	for(a=0; a<twait_a; a++) {
		if(twait_wait[a] < min_wait) {
			min_wait = twait_wait[a];
		}
	}
	if(min_wait < 0) {
		if(flush_size == 0) {
			holding_disk -= min_wait;
			holding_disk -= space_change;
		}
		for(a=0; a<twait_a; a++) {
			twait_wait[a] -= min_wait;
		}
	}
	if (holding_disk != 0) {
		const = 100/holding_disk;
	}
	else {
		const = 100;
	}
	for(a=0; a<disk_a; ++a) {
		space = (holding_disk - disk_alloc_space[a])*const+disk_raise
		printf plot_fmt1, disk_alloc_time[a], space >> "disk_alloc";
	}
	for(a=0; a<twait_a; ++a) {
		space = (twait_wait[a])*const+disk_raise
		printf plot_fmt1, twait_time[a], space >> "tape_wait";
	}

	printf plot_fmt, tim, active_old, tim, dump_raise >>"dump_idle";
	printf plot_fmt, tim, state_old, tim, tape_raise >>"tape_idle";	
	printf plot_fmt, tim, unused_old, tim, bandw_raise >>"bandw_free";
	printf plot_fmt, tim, finish_old, tim, written*count_scale+que_raise >>"finished";
	printf plot_fmt, tim, run_old, tim, run_old >>"run_queue";
}

function do_result(){		# process lines driver: result
	if($7=="DONE" ) {
		if( $6=="taper:"){ 		# taper done
			tsize -= $14;	
			tout  += $14;
			tcnt--;	written++;
		}
		else { 				# dumperx done 
		  tsize += (int($15/32)+1)*32; 	# in tape blocks 
		  tcnt++;	done++;
		  xx = host[$6];
		  d = disk[$6];
		  l = level[$6];
		  host_time[xx]+= ( tt = $4 - dmpr_strt[$6]);
		  if(xx in disk_list) disk_list[xx] = disk_list[xx] "\n";
		  disk_list[xx] = disk_list[xx] \
				  xx ":" d "/" l "\t" \
				  pr_time(dmpr_strt[$6]) \
				  " - " pr_time($4) \
				  " = "  pr_time(tt);
#		  print host[$6], disk[host[$6]];
#			print host[$6], $4, dmpr_strt[$6], host_time[host[$6]]
		}
	}
	else if ($6=="taper:") {		# something else than DONE
		if($7=="TAPE-ERROR" || $7=="TRY-AGAIN") {
			tape_err= 1;
			err_time=$4/time_scale;
		}
		else if ($7=="TAPER-OK") tape_err=0;
		else if ($7=="PORT")    tape_err=0;
		else if ($7=="REQUEST-NEW-TAPE")    tape_err=0;
		else if ($7=="NEW-TAPE")    tape_err=0;
		else if ($7=="PARTDONE")    tape_err=0;
		else if ($7=="DUMPER-STATUS")    tape_err=0;
		else print fil, "UNKNOWN STATUS# "$0 ;
	}
	else { 					# something bad from dumper 
		if ($7=="FAILED") { failed++;}
		else if ($7=="TRY-AGAIN"){ try++;}
		else if ($7=="PORT") ;  # ignore from chunker
		else if ($7=="RQ-MORE-DISK") ;  # FIXME: ignore for now
		else if ($7=="NO-ROOM")  
		  print fil, pr_time($4),"#"  ++no_room, $0;
		else if( $7=="ABORT-FINISHED") print fil, "#" ++no_abort, $0;
		else print fil, "UNKNOWN STATUS# " $0;
	}
}

function do_moves() { # function that extracts the estimated size of dumps
		      # by processing DELAYING and promoting lines
	est_size=$6;
	getline ;			# eat get next line print out planner msg
	while (NF > 0 && (($1 == "delay:") || ($1 == "planner:")) ) {
	  if( $1 == "delay:") est_size = $NF; 	# processing delay lines
	  else print fil, "DELAY#", $0;
	  getline;
	}
	getline ; 			# eating blank line
	if( $1== "PROMOTING") {         # everything is dandy 
		getline;		# get first promote line
		while ( NF>0 && ($1 == "promote:" || $1 == "planner:" || $1 == "no" || $1 == "try") ) {
			if( $2 == "moving") {
				est_size=$8;
				print fil, "PROMOTING#", $1, $3;
			}
			else if($2 != "checking" && $2 != "can't" && $3 != "too" &&  $1 != "no" && $1 != "try" && $2 != "time")
			     print fil,"PROMOTING#", $0;
			getline ;	# get next promote line
		}
	}
	else print fil, "DID NOT FIND PROMOTING LINE IN THE RIGHT PLACE",NR,$0;
}


END {
	if( holding_disk == -1) { 		# bad input file 
		print fil,": MISSING SPACE DECLARATION" ;
		exit;
	}
# print headers of each graph  this is for the gnulot version 
	if( tim >maxtime && extend==0)# if graph will extend beond borders
	  printf "Graph extends beond borders %s taking %7.3f > (max = %7.3f)\n",
			fil, tim, maxtime ;
	print_t();			# print titles
	if( no_room + no_abort > 0) 
	     printf "NO-ROOM=%5d ABORT-FINISHED=%5d\n",  no_room, no_abort;
	max_out = 20;
	old_t = min_host * min_host;  # Some thing big
	print "Longest dumping hosts   Times", min_host;
	print "Host:disk/lev  \t start  -   end   =   run\t=> total";
	while ( max_out-- > 0 && old_t > min_host) {
	  t = 0;
	  for (j in host_time) {
	    if( t < host_time[j] && host_time[j] <old_t){
	      t = host_time[d=j];
	    }
	  }
	  printf "%s\t=> %s\n\n", disk_list[d], pr_time(host_time[d]);
#	  printf "%-20.20s Total Dump time %s\n", d, pr_time(host_time[d]);
	  old_t = t;
	}
}

function print_t(){		# printing out the labels for the graph 
	label=0;		# calculating where labels go and 
				# range for x and y axes
	maxy = int(no_disks/60+1)*20+que_raise;
	printf "set yrange[0:%d]\n",maxy >"title";
	if( maxtime < tim && extend !=0) {
		printf "set xrange[0:%d]\n", tim+30 >>"title";
		first_col = 10;
		second_col = (tim+30) * 0.45;
		key_col = (tim+30) * 1.042;
		third_col = (tim+30) * 1.0125;
	}
	else {
		printf "set xrange[0:%d]\n", maxtime >>"title";
		first_col = maxtime * 0.042
		second_col = maxtime * 0.45
		key_col = maxtime;
		third_col = maxtime*1.0125;
	}
	label_shift = (7 + int(no_disks/100));
	lab = label_start = maxy+(6*label_shift) ;  # showing 6 labels
	printf "set key at %d, %d\n", key_col, lab+4 >>"title";
	printf "set label %d \"Amanda Dump %s\" at %d,%d\n", ++label,fil, 
		first_col,lab >"title";
	lab -= label_shift;
	printf "set label %d \"Bandwidth = %d\" at %d,%d\n",++label,bandw,
		first_col,lab >>"title";

	lab -= label_shift;
	printf "set label %d \"Holding disk = %d\" at %d,%d\n",++label,size,
		first_col,lab >>"title";

	lab -= label_shift;
	printf "set label %d \"Tape Policy = %s\" at %d,%d\n",++label,policy,
		first_col,lab >>"title";

	lab -= label_shift;
	printf "set label %d \"Dumpers= %d\" at %d,%d\n",++label,dumpers,
		first_col,lab >>"title";

	lab -= label_shift;
	if( alg =="drain-ends") 
		printf "set label %d \"Driver alg = %s At big end %d\" at %d,%d\n",
			++label,alg, big,first_col,lab >>"title";
	else #if( alg =="InOrder")  # other special cases
		printf "set label %d \"Driver alg = %s\" at %d,%d\n",
			++label,alg,first_col, lab >>"title";

	lab = label_start;
	printf "set label %d \"Elapsed Time = %s\" at %d,%d\n",
		++label,pr_time(tim*60),second_col,lab >>"title";

	lab -= label_shift;
	if( tape_err==1)	stm = "TAPE ERROR";
	else if( quit ==1)	stm = "SUCCESS";
	else  {			stm = "UNKNOWN";
		print "Unknown terminating status",fil;
	}
	printf "set label %d \"Final status = %s\" at %d,%d\n",
		++label,stm, second_col,lab >> "title";

	lab -= label_shift;
	printf "set label %d \"Dumped/Failed = %3d/%d\" at %d,%d\n",
		++label,done,(failed+(try/2)), second_col,lab >>"title";

	lab -= label_shift; 
	printf "set label %d \"Output data size = %d\" at %d, %d\n",
		++label,int(tout/1024+0.49999),second_col,lab >>"title";
	if( est_size >0) {
		lab -= label_shift; 
		printf "set label %d \"Estimated data size = %d\" at %d, %d\n",
			++label,int(est_size/1024+0.49999),second_col,lab >>"title";
	}

	if (gnuplot==0) {
		printf "set output \"%s.ps\"\n",fil >>"title";
		if(bw==1) {
			if(paper==1) printf "set term postscript landscape \"Times-Roman\" 10\n" >>"title";
			else printf "set term postscript portrait \"Times-Roman\" 10\n" >>"title";
		}
		else {
			if(paper==1) printf "set term postscript landscape color \"Times-Roman\" 10\n" >>"title";
			else printf "set term postscript portrait color \"Times-Roman\" 10\n" >>"title";
		}
	} else {
		printf "set term x11\n" >> "title";
	}
	printf "set ylabel """";" >>"title"; 	# make sure there is no ylabel
	fmt= "set label %d \"%s\" at "third_col", %d\n";
	printf fmt, ++label,"%DUMPERS", 40 >>"title";
	printf fmt, ++label,"TAPE",  95 >>"title";
	printf fmt, ++label,"HOLDING",180 >>"title";
	printf fmt, ++label,"DISK", 160 >>"title";
	printf fmt, ++label,"%BANDWIDTH", 260 >>"title";
	printf fmt, ++label,"QUEUES",(que_raise+maxy)/2 >>"title";
	if((paper+gnuplot) > 0)	print "set size 0.9, 0.9;"  >>"title";
	else			print "set size 0.7,1.3;"   >>"title";
}

function pr_time(pr_a){ #function to pretty print time
  pr_h = int(pr_a/3600);
  pr_m = int(pr_a/60)%60;
  pr_s = int(pr_a+0.5) %60;
  if( pr_m < 10 && pr_s < 10 ) return  pr_h":0"pr_m":0"pr_s;
  else if( pr_s < 10)          return  pr_h":" pr_m":0"pr_s;
  else if( pr_m < 10)          return  pr_h":0"pr_m":" pr_s;
  else                         return  pr_h":" pr_m":" pr_s;
}
