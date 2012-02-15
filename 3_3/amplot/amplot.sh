#!@SHELL@
# Amanda, The Advanced Maryland Automatic Network Disk Archiver
# Copyright (c) 1992-1998 University of Maryland at College Park
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
# Author: Olafur Gudumundsson, (ogud@tis.com)  Trusted Information Systems
# Formerly at: 		  Systems Design and Analysis Group
#			  Computer Science Department
#			  University of Maryland at College Park
#
#	Amplot: a program to generate postscript plots of each nights amanda 
#	performance 
# 
#	Author: Olafur Gudmundsson (ogud@tis.com) 
#	Creation Date: April 1992 
#	Last modified: April 1995 
#	Input: list of amdumps 
#	Output: Plot of amdump files as either gnuplots on the screen or
#		Postscript files 
#

prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"
 
confdir=@CONFIG_DIR@
 
# add sbin and ucb dirs
PATH="$PATH:/usr/sbin:/sbin:/usr/ucb"
export PATH

# we use a different version of the compression variable than amanda itself.
COMPRESS=@AMPLOT_COMPRESS@

# Function to check that awk can do command-line variable
# substitution.  If no, then exit; if yes, set $AVARFLAG
# to the commandline switch used to introduce a variable. This
# check used to be performed at build time in configure; it's
# now performed at runtime.
test_awk() {
	local tmpfile result
	tmpfile=`mktemp /tmp/amplot.XXXXXX`
	echo 'BEGIN{print i; exit}' > ${tmpfile}
	result=`$AWK -f ${tmpfile} i=xx | wc -c`
	if test "$result" -le 1; then
		result=`$AWK -f ${tmpfile} -v i=xx | wc -c`
		if test "$result" -le 1; then
			echo "$AWK does not support command-line variable assignment; amplot cannot run" >&2
			rm -fr $tmpfile
			exit 1
		else
			AVARFLAG=-v
		fi
	else
		AVARFLAG=''
	fi

	rm -fr $tmpfile
}

# Function to search for gnuplot and ensure it's working.  This
# first tries the location detected/configured when amanda was built,
# then tries 'gnuplot', assuming it's in the user's path.  If no
# working gnuplot executable is found, it exits with an error.  The
# variable $GNUPLOT is set to the resulting executable.
find_gnuplot() {
	if test "x$GNUPLOT" = "x"; then
		# look for it in the user's PATH
		GNUPLOT=gnuplot
	fi

	if ${GNUPLOT} --version 2>/dev/null | grep '^gnuplot' >/dev/null; then
		: # looks OK
	else
		echo "${GNUPLOT} was not found; amplot cannot run"
		exit 1
	fi
}

# check our environment, using functions from above
test_awk
find_gnuplot

if [ $# -eq 0 ] ; then
	_ 'Usage: %s [-c] [-e] [-g] [-l] [-p] [-t hours] <amdump_files.[gz,z,Z]>\n' $0
	_ '%s generates plot for screen with fixed dimensions\n' $0
	_ '	-c	Compress the input amdump files after plotting\n'
	_ '	-e	Extends x (time) axes if needed\n'
	_ '	-g	Run gnuplot directly no postscript file generated DEFAULT\n'
	_ '	-l	Landscape mode suitable for printing\n'
	_ '	-p	Postscript output (color)\n'
	_ '	-b	The postscipt will be b/w\n'
	_ '	-t T	Set the right edge of the plot to be T hours\n'
	exit 1 
fi

tmp_files="bandw_free disk_alloc dump_idle finished run_queue tape_* title" 

my_plot=$amlibexecdir/amplot.g
paper=0 
gnuplot=1
cmpres=0
para=""
maxtime=4
bw=0

# setting up the parameters to pass to [gn]awk 
while :; do 
   case "$1" in
   -c)  cmpres=1; shift;;
   -e)  para=$para"$AVARFLAG extend=1 "; shift;;
   -g)  gnuplot=1; shift;;
   -l)  paper=1; para=$para"$AVARFLAG paper=1 "; shift;;
   -p)  gnuplot=0; shift;;
   -b)	bw=1; shift;;
   -t)  shift
	if test "$#" -eq 0; then
	    _ '%s: no argument for -t option\n' $0 1>&2
	    exit 5
	fi
	maxtime="$1"; shift;;
   *) break;;
   esac
done
if [ $# -eq 0 ] ; then 
	_ '%s: no input files\n' $0 1>&2
	exit 5
fi
para=$para"$AVARFLAG maxtime=$maxtime"

if [ $gnuplot  -eq 1 ] ; then
	my_plot=$my_plot"p"		# use the plot prog that pauses
	plot=" -geometry 800x700+40+0" 
	para=$para"$AVARFLAG gnuplot=1 "
	_ "Displaying graph on the screen, <CR> for next graph"

	if [ "$paper" -eq 1 ] ; then
		_ '%s: -l requires -p flag at the same time\n' $0 1>&2
		exit 6 
	fi
	if [ "$bw" -eq 1 ] ; then
		_ '%s: -b requires -p flag at the same time\n' $0 1>&2
		exit 6 
	fi
fi

if [ $bw -eq 1 ]; then
	para=$para" bw=1"
fi

list="";		# files to compress at the end

for i in ${1+"$@"}		# for all the input files
do
	f="$i";
	if [ ! -f "$f" ] ; then 
		f=`ls "$i" "$i".*[zZ] 2>/dev/null`
	fi
	if [ -f "$f" ] ; then 		# found file 
                disp=`$AWK -f $amlibexecdir/amcat.awk $AVARFLAG f="$f"`
		if [ -z "$disp" ] ; then 
			_ 'Do not know how to [gz|z]cat this file\n'
		else
			/bin/rm -f $tmp_files 
			$disp "$f" | $AWK -f $amlibexecdir/amplot.awk $para
			$GNUPLOT $plot $my_plot
			if [ $disp = "cat" -a  $cmpres -eq 1 ] ; then
				list=$list" "$f
			fi
		fi
	else 				# check if file has been compressed
		_ 'No such file %s or %s\n' "$i" "$i.*[zZ]"
	fi
done

/bin/rm -f $tmp_files 

if [ "$list" != "" ] ; then		# now compress the files we worked on
# comment out next line if you do not want compression at the end
	_ 'Compressing %s\n' "$list"
	$COMPRESS $list
fi
exit 0
