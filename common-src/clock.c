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
static struct timeval timesub(struct timeval end, struct timeval start);
static struct timeval timeadd(struct timeval a, struct timeval b);

times_t times_zero;
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
    amanda_timezone dontcare;

    clock_running = 1;
    amanda_gettimeofday(&start_time.r, &dontcare);
}

times_t
stopclock(void)
{
    times_t diff;
    struct timeval end_time;
    amanda_timezone dontcare;

    if(!clock_running) {
	fprintf(stderr,"stopclock botch\n");
	exit(1);
    }
    amanda_gettimeofday(&end_time, &dontcare);
    diff.r = timesub(end_time,start_time.r);
    clock_running = 0;
    return diff;
}

times_t
curclock(void)
{
    times_t diff;
    struct timeval end_time;
    amanda_timezone dontcare;

    if(!clock_running) {
	fprintf(stderr,"curclock botch\n");
	exit(1);
    }
    amanda_gettimeofday(&end_time, &dontcare);
    diff.r = timesub(end_time,start_time.r);
    return diff;
}

times_t
timesadd(
    times_t	a,
    times_t	b)
{
    times_t sum;

    sum.r = timeadd(a.r,b.r);
    return sum;
}

times_t
timessub(
    times_t	a,
    times_t	b)
{
    times_t dif;

    dif.r = timesub(a.r,b.r);
    return dif;
}

char *
times_str(
    times_t	t)
{
    static char str[10][NUM_STR_SIZE+10];
    static size_t n = 0;
    char *s;

    /* tv_sec/tv_usec are longs on some systems */
    snprintf(str[n], SIZEOF(str[n]), "rtime %lu.%03lu",
	     (unsigned long)t.r.tv_sec,
	     (unsigned long)t.r.tv_usec / 1000);
    s = str[n++];
    n %= am_countof(str);
    return s;
}

char *
walltime_str(
    times_t	t)
{
    static char str[10][NUM_STR_SIZE+10];
    static size_t n = 0;
    char *s;

    /* tv_sec/tv_usec are longs on some systems */
    snprintf(str[n], SIZEOF(str[n]), "%lu.%03lu",
	     (unsigned long)t.r.tv_sec,
	     (unsigned long)t.r.tv_usec/1000);
    s = str[n++];
    n %= am_countof(str);
    return s;
}

static struct timeval
timesub(
    struct timeval	end,
    struct timeval	start)
{
    struct timeval diff;

    if(end.tv_usec < start.tv_usec) { /* borrow 1 sec */
	if (end.tv_sec > 0)
	    end.tv_sec -= 1;
	end.tv_usec += 1000000;
    }
    diff.tv_usec = end.tv_usec - start.tv_usec;
    diff.tv_sec = end.tv_sec - start.tv_sec;
    return diff;
}

static struct timeval
timeadd(
    struct timeval	a,
    struct timeval	b)
{
    struct timeval sum;

    sum.tv_sec = a.tv_sec + b.tv_sec;
    sum.tv_usec = a.tv_usec + b.tv_usec;

    if(sum.tv_usec >= 1000000) {
	sum.tv_usec -= 1000000;
	sum.tv_sec += 1;
    }
    return sum;
}
