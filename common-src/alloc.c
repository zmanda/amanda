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

#define	MIN_ALLOC	64

static char *internal_vstralloc(const char *, int, const char *, va_list);

/*
 * newalloc - free existing buffer and then g_malloc a new one.
 */
void *
debug_newalloc(
    const char *file G_GNUC_UNUSED,
    int		line G_GNUC_UNUSED,
    void *	old,
    size_t	size)
{
    char *addr;

    addr = g_malloc(size);
    amfree(old);
    return addr;
}


/*
 * internal_vstralloc - copies up to MAX_STR_ARGS strings into newly
 * allocated memory.
 *
 * The MAX_STR_ARGS limit is purely an efficiency issue so we do not have
 * to scan the strings more than necessary.
 */

#define	MAX_VSTRALLOC_ARGS	40

static char *
internal_vstralloc(
    const char *file,
    int		line,
    const char *str,
    va_list argp)
{
    char *next;
    char *result;
    int a, b;
    size_t total_len;
    const char *arg[MAX_VSTRALLOC_ARGS+1];
    size_t len[MAX_VSTRALLOC_ARGS+1];
    size_t l;

    if (str == NULL) {
	errordump(_("internal_vstralloc: str is NULL"));
	/*NOTREACHED*/
    }

    a = 0;
    arg[a] = str;
    l = strlen(str);
    total_len = len[a] = l;
    a++;

    while ((next = arglist_val(argp, char *)) != NULL) {
	if ((l = strlen(next)) == 0) {
	    continue;				/* minor optimisation */
	}
	if (a >= MAX_VSTRALLOC_ARGS) {
	    errordump(_("%s@%d: more than %d args to vstralloc"),
		      file ? file : _("(unknown)"),
		      file ? line : -1,
		      MAX_VSTRALLOC_ARGS);
	    /*NOTREACHED*/
	}
	arg[a] = next;
	len[a] = l;
	total_len += l;
	a++;
    }

    result = g_malloc(total_len+1);

    next = result;
    for (b = 0; b < a; b++) {
	memcpy(next, arg[b], len[b]);
	next += len[b];
    }
    *next = '\0';

    return result;
}


/*
 * newstralloc - free existing string and then g_strdup a new one.
 */
char *
debug_newstralloc(
    const char *file G_GNUC_UNUSED,
    int		line G_GNUC_UNUSED,
    char *	oldstr,
    const char *newstr)
{
    char *addr;

    addr = g_strdup(newstr);
    amfree(oldstr);
    return (addr);
}


/*
 * newvstralloc - free existing string and then vstralloc a new one.
 */
char *
debug_newvstralloc(
    const char *file,
    int		line,
    char *	oldstr,
    const char *newstr,
    ...)
{
    va_list argp;
    char *result;

    arglist_start(argp, newstr);
    result = internal_vstralloc(file, line, newstr, argp);
    arglist_end(argp);
    amfree(oldstr);
    return result;
}


/*
 * newvstrallocf - free existing string and then g_strdup_printf a new one.
 */
char *
debug_newvstrallocf(
    const char *file G_GNUC_UNUSED,
    int		line G_GNUC_UNUSED,
    char *	oldstr,
    const char *fmt,
    ...)
{
    size_t	size;
    char *	result;
    va_list	argp;

    result = g_malloc(MIN_ALLOC);
    if (result != NULL) {

	arglist_start(argp, fmt);
	size = g_vsnprintf(result, MIN_ALLOC, fmt, argp);
	arglist_end(argp);

	if (size >= MIN_ALLOC) {
	    amfree(result);
	    result = g_malloc(size + 1);

	    arglist_start(argp, fmt);
	    (void)g_vsnprintf(result, size + 1, fmt, argp);
	    arglist_end(argp);
	}
    }
    amfree(oldstr);
    return result;
}

/* vstrextend -- Extends the existing string by appending the other 
 * arguments. */
char *
debug_vstrextend(
    const char *file,
    int		line,
    char **	oldstr,
    ...)
{
	char *keep = *oldstr;
	va_list ap;

	arglist_start(ap, oldstr);

	if (*oldstr == NULL)
		*oldstr = "";
	*oldstr = internal_vstralloc(file, line, *oldstr, ap);
        amfree(keep);

	arglist_end(ap);
        return *oldstr;
}

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
