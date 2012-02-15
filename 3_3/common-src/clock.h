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
 * $Id: clock.h,v 1.6 2006/05/25 01:47:11 johnfranks Exp $
 *
 * interface for timing functions
 */
#ifndef CLOCK_H
#define CLOCK_H

#include "amanda.h"

typedef GTimeVal times_t;

/* NOT THREAD SAFE */
void startclock(void);
times_t stopclock(void);
times_t curclock(void);
char * walltime_str(times_t t);
int clock_is_running(void);

/* Thread safe */
times_t timeadd(times_t a, times_t b);
#define timesadd(x, y) timeadd(x, y)

times_t timesub(times_t a, times_t b);
#define timessub(x, y) timesub(x, y)

double g_timeval_to_double(GTimeVal v);

void amanda_gettimeofday(struct timeval * timeval_time);


#endif /* CLOCK_H */
