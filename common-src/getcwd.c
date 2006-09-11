/* 
 * getcwd.c --
 *
 *	This file provides an implementation of the getcwd procedure
 *	that uses getwd, for systems with getwd but without getcwd.
 *
 * Copyright (c) 1993 The Regents of the University of California.
 * All rights reserved.
 *
 * Permission is hereby granted, without written agreement and without
 * license or royalty fees, to use, copy, modify, and distribute this
 * software and its documentation for any purpose, provided that the
 * above copyright notice and the following two paragraphs appear in
 * all copies of this software.
 * 
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES ARISING OUT
 * OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE UNIVERSITY OF
 * CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATION TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

/* $Id: getcwd.c,v 1.5 2006/05/25 01:47:12 johnfranks Exp $ */

#ifndef lint
static char rcsid[] = "$Header: /cvsroot/amanda/amanda/common-src/getcwd.c,v 1.5 2006/05/25 01:47:12 johnfranks Exp $ SPRITE (Berkeley)";
#endif /* not lint */

#include "amanda.h"

extern char *getwd();
extern int errno;

char *
getcwd(buf, size)
    char *buf;			/* Where to put path for current directory. */
    size_t size;	          	/* Number of bytes at buf. */
{
    char realBuffer[MAXPATHLEN+1];
    int length;

    if (getwd(realBuffer) == NULL) {
	/*
	 * There's not much we can do besides guess at an errno to
	 * use for the result (the error message in realBuffer isn't
	 * much use...).
	 */

	errno = EACCES;
	return NULL;
    }
    length = strlen(realBuffer);
    if (length >= size) {
	errno = ERANGE;
	return NULL;
    }
    strncpy(buf, realBuffer, size-1);
    buf[size-1] = '\0';
    return buf;
}
