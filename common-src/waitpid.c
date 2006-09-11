/*
 
Copyright (c) 1990, 1991, 1992  X Consortium

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
X CONSORTIUM BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of the X Consortium shall not be
used in advertising or otherwise to promote the sale, use or other dealings
in this Software without prior written authorization from the X Consortium.

 *
 * Copyright 1990, 1991, 1992 by UniSoft Group Limited.
 * 
 * Permission to use, copy, modify, distribute, and sell this software and
 * its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appear in all copies and that
 * both that copyright notice and this permission notice appear in
 * supporting documentation, and that the name of UniSoft not be
 * used in advertising or publicity pertaining to distribution of the
 * software without specific, written prior permission.  UniSoft
 * makes no representations about the suitability of this software for any
 * purpose.  It is provided "as is" without express or implied warranty.
 *
 * $XConsortium: waitpid.c,v 1.3 94/04/17 20:59:56 rws Exp $
 */

/* modified for Amanda team:
 * replaced all occurrences of "int" for status variables with "amwait_t"
 * compile only if wait4() is not available; if it is, waitpid() is
 * #defined to use wait4 instead.  */
 
#include "config.h"

#ifdef HAVE_WAIT4
/* some compilers do not't like empty C sources */
static void foo() {};

#else

#if 1
#include "amanda.h"
#else
/* these are all included in amanda.h */
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#endif

#ifdef DEBUG
#   include <stdio.h>
#   define	DBP(fmt,arg)	(fprintf(stderr, "waitpid(%d): ", currpid), \
				 fprintf(stderr, fmt, arg), fflush(stderr))
#else
#   define	DBP(fmt,arg)
#endif

#define MAXTAB 20

static struct {
	pid_t pid;
	amwait_t /*int*/ status;
} savtab[MAXTAB];
static int	ntab = 0;
static pid_t	currpid = 0;

pid_t
waitpid(pid, stat_loc, options)
pid_t pid;
amwait_t /*int*/ *stat_loc;
int options;
{
	int rval, i;
	amwait_t /*int*/ local_loc;

	/* Clear the table for each new process */
	if (getpid() != currpid)
	{
		ntab = 0;
		currpid = getpid();
		DBP("clearing table\n", 0);
	}

	DBP("waiting for pid %d\n", pid);

	if (options & ~(WNOHANG|WUNTRACED))
	{
		errno = EINVAL;
		return -1;
	}

	if (pid == -1)
	{
		/* see if any saved */
		for (i=0; i<ntab; i++)
			if (savtab[i].pid != 0)
			{
				pid = savtab[i].pid;
				if (stat_loc)
					*stat_loc = savtab[i].status;
				savtab[i].pid = 0;
				DBP("got %d from table\n", pid);
				return pid;
			}

		DBP("return wait3(...)\n", 0);
		return wait3(stat_loc, options, (int *)0);
	}

	if (pid <= 0)
	{
		/* can't do this functionality (without reading /dev/kmem!) */
		errno = EINVAL;
		return -1;
	}

	/* see if already saved */
	for (i=0; i<ntab; i++)
		if (savtab[i].pid == pid)
		{
			if (stat_loc)
				*stat_loc = savtab[i].status;
			savtab[i].pid = 0;
			DBP("found %d in table\n", pid);
			return pid;
		}

	rval = wait3(&local_loc, options, (int *)0);
	DBP("wait3() returned %d\n", rval);
	while (rval != pid && rval != -1 && rval != 0)
	{
		/* save for later */
		for (i=0; i<ntab; i++)
			if (savtab[i].pid == 0)
			{
				savtab[i].pid = rval;
				savtab[i].status = local_loc;
				DBP("saved %d in free slot\n", rval);
				break;
			}
		if (i == ntab)
		{
			if (ntab < MAXTAB)
			{
				savtab[ntab].pid = rval;
				savtab[ntab].status = local_loc;
				++ntab;
				DBP("saved %d in new slot\n", rval);
			}
			else
				DBP("no free slot for %d\n", rval);
		}
		rval = wait3(&local_loc, options, (int *)0);
		DBP("wait3() returned %d\n", rval);
	}

	if(stat_loc)
		*stat_loc = local_loc;

	return rval;
}
#endif
