/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
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
 * Authors: the Amanda Development Team.  Its members are listed in a
 * file named AUTHORS, in the root directory of this distribution.
 */
/*
 * $Id: g_malloc.c 5280 2007-02-13 15:58:56Z martineau $
 *
 * Memory allocators with error handling.  If the allocation fails,
 * errordump() is called, relieving the caller from checking the return
 * code
 */
#include "amanda.h"

/*
 * safe_env_full - build a "safe" environment list.
 */
char **
safe_env_full(char **add)
{
    static char *safe_env_list[] = {
	"TZ",
#ifdef __CYGWIN__
	"SYSTEMROOT",
#endif
#ifdef NEED_PATH_ENV
	"PATH",
#endif
	"DISPLAY",
	NULL
    };

    /*
     * If the initial environment pointer malloc fails, set up to
     * pass back a pointer to the NULL string pointer at the end of
     * safe_env_list so our result is always a valid, although possibly
     * empty, environment list.
     */
    char **envp = safe_env_list + G_N_ELEMENTS(safe_env_list) - 1;

    char **p;
    char **q;
    char *s;
    char *v;
    size_t l1, l2;
    char **env;
    int    env_cnt;
    int nadd = 0;

    /* count ADD */
    for (p = add; p && *p; p++)
	nadd++;

    if (getuid() == geteuid() && getgid() == getegid()) {
	env_cnt = 1;
	for (env = environ; *env != NULL; env++)
	    env_cnt++;
	if ((q = (char **)malloc((nadd+env_cnt)*sizeof(char *))) != NULL) {
	    envp = q;
	    p = envp;
	    /* copy in ADD */
	    for (env = add; env && *env; env++) {
		*p = *env;
		p++;
	    }
	    for (env = environ; *env != NULL; env++) {
		if (strncmp("LANG=", *env, 5) != 0 &&
		    strncmp("LC_", *env, 3) != 0) {
		    *p = g_strdup(*env);
		    p++;
		}
	    }
	    *p = NULL;
	}
	return envp;
    }

    if ((q = (char **)malloc(nadd*sizeof(char *) + sizeof(safe_env_list))) != NULL) {
	envp = q;
	/* copy in ADD */
	for (p = add; p && *p; p++) {
	    *q = *p;
	    q++;
	}

	/* and copy any SAFE_ENV that are already set */
	for (p = safe_env_list; *p != NULL; p++) {
	    if ((v = getenv(*p)) == NULL) {
		continue;			/* no variable to dup */
	    }
	    l1 = strlen(*p);			/* variable name w/o null */
	    l2 = strlen(v) + 1;			/* include null byte here */
	    if ((s = (char *)malloc(l1 + 1 + l2)) == NULL) {
		break;				/* out of memory */
	    }
	    *q++ = s;				/* save the new pointer */
	    memcpy(s, *p, l1);			/* left hand side */
	    s += l1;
	    *s++ = '=';
	    memcpy(s, v, l2);			/* right hand side and null */
	}
	*q = NULL;				/* terminate the list */
    }
    return envp;
}
