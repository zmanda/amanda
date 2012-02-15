/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * All Rights Reserved.
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of U.M. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  U.M. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 *
 * U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
 * BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: clock.c,v 1.7 2006/07/27 18:12:10 martinea Exp $
 *
 * timing functions
 */
#include "amanda.h"

#include "clock.h"

/* local functions */
times_t start_time;
static int clock_running = 0;

int
clock_is_running(void)
{
    return clock_running;
}

void
startclock(void)
{
    clock_running = 1;
    
    g_get_current_time(&start_time);
}

times_t
stopclock(void)
{
    GTimeVal diff;

    diff = curclock();

    clock_running = 0;
    return diff;
}

times_t
curclock(void)
{
    GTimeVal end_time;

    if(!clock_running) {
	g_fprintf(stderr,_("curclock botch\n"));
	exit(1);
    }

    g_get_current_time(&end_time);
    return timesub(end_time,start_time);
}

char *
walltime_str(
    times_t	t)
{
    static char str[10][NUM_STR_SIZE+10];
    static size_t n = 0;
    char *s;

    /* tv_sec/tv_usec are longs on some systems */
    g_snprintf(str[n], SIZEOF(str[n]), "%lu.%03lu",
	     (unsigned long)t.tv_sec,
	     (unsigned long)t.tv_usec/1000);
    s = str[n++];
    n %= am_countof(str);
    return s;
}

GTimeVal timesub(GTimeVal end, GTimeVal start) {
    GTimeVal diff;

    if(end.tv_usec < start.tv_usec) { /* borrow 1 sec */
	if (end.tv_sec > 0)
	    end.tv_sec -= 1;
	end.tv_usec += 1000000;
    }
    diff.tv_usec = end.tv_usec - start.tv_usec;

    if (end.tv_sec > start.tv_sec)
	diff.tv_sec = end.tv_sec - start.tv_sec;
    else
	diff.tv_sec = 0;

    return diff;
}

GTimeVal timeadd(GTimeVal a, GTimeVal b) {
    GTimeVal sum;

    sum.tv_sec = a.tv_sec + b.tv_sec;
    sum.tv_usec = a.tv_usec + b.tv_usec;

    if(sum.tv_usec >= 1000000) {
	sum.tv_usec -= 1000000;
	sum.tv_sec += 1;
    }
    return sum;
}

double g_timeval_to_double(GTimeVal v) {
    return v.tv_sec + ((double)v.tv_usec) / G_USEC_PER_SEC;
}

void amanda_gettimeofday(struct timeval * timeval_time) {
    GTimeVal gtimeval_time;

    g_get_current_time(&gtimeval_time);
    timeval_time->tv_sec = gtimeval_time.tv_sec;
    timeval_time->tv_usec = gtimeval_time.tv_usec;
}
