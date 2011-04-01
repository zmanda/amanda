#
# Amanda, The Advanced Maryland Automatic Network Disk Archiver
# Copyright (c) 1992,1993.1994-1998 University of Maryland at College Park
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
# program to print the summary graph for the amanda runs in gnupot
# gnumaster.awk will generate the data for this one 
#	Creation Date July 1992
#	Last modified: May 1993
#	Input: the files specified below on the  plot line
#	Output: a postscript file 
#
set style data lines
set xrange [0:210]
set yrange [0:420]
set xlabel "Minutes"
#set xtics 0,10
set xtics ( \
    "0:00"   0, ""  10, "0:20"  20, ""  30, "0:40"  40, ""  50,\
    "1:00"  60, ""  70, "1:20"  80, ""  90, "1:40" 100, "" 110,\
    "2:00" 120, "" 130, "2:20" 140, "" 150, "2:40" 160, "" 170,\
    "3:00" 180, "" 190, "3:20" 200, "" 210, "3:40" 220, "" 230,\
    "4:00" 240, "" 250, "4:20" 260, "" 270, "4:40" 280, "" 290,\
    "5:00" 300, "" 310, "5:20" 320, "" 330, "5:40" 340, "" 350,\
    "6:00" 360, "" 370, "6:20" 380, "" 390, "6:40" 400, "" 410,\
    "7:00" 420, "" 430, "7:20" 440, "" 450, "7:40" 460, "" 470,\
    "8:00" 480, "" 490, "8:20" 500, "" 510, "8:40" 520, "" 530)

set ytics ("0" 0, "20" 15, "40" 30, "60" 45, "80" 60, "100" 75,\
	"Idle" 90,"Active" 100, \
	"0" 120, "20" 140,"40" 160, "60" 180, "80" 200, "100" 220,\
	"0" 250, "100" 280,\
	"0"   300, "60"  320, "120" 340, "180" 360, "240" 380, "300" 400,\
	"360" 420, "420" 440, "480" 460, "540" 480, "600" 500)

#set size 0.7,1.3; set term postscript portrait "Times-Roman" 10
#set size 0.9,0.9; set term postscript landscape "Times-Roman" 12
# file title has the parameters that this program needs
load 'title'
plot 	"run_queue" title "Run Queue" with lines,\
	"tape_queue" title "Tape Queue" with lines,\
	"finished"  title "Dumps Finished" with lines,\
	"bandw_free" title "Bandwidth Allocated" with lines, \
	"disk_alloc" title "%Disk Allocated" with lines, \
	"tape_wait" title "%Tape Wait" with lines,\
	"tape_idle" title "Taper Idle" with lines,\
	"dump_idle" title "Dumpers Idle" with lines
