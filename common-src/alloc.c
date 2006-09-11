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
 * $Id: alloc.c,v 1.37 2006/07/05 10:41:32 martinea Exp $
 *
 * Memory allocators with error handling.  If the allocation fails,
 * errordump() is called, relieving the caller from checking the return
 * code
 */
#include "amanda.h"
#include "arglist.h"
#include "queue.h"

static char *internal_vstralloc(const char *, va_list);

/*
 *=====================================================================
 * debug_caller_loc -- keep track of all allocation callers
 *
 * const char *debug_caller_loc(const char *file, int line)
 *
 * entry:	file = source file
 *		line = source line
 * exit:	a string like "genversion.c@999"
 *
 * The debug malloc library has a concept of a call stack that can be used
 * to fine tune what was running when a particular allocation was done.
 * We use it to tell who called our various allocation wrappers since
 * it wouldn't do much good to tell us a problem happened because of
 * the malloc call in alloc (they are all from there at some point).
 *
 * But the library expects the string passed to malloc_enter/malloc_leave
 * to be static, so we build a linked list of each one we get (there are
 * not really that many during a given execution).  When we get a repeat
 * we return the previously allocated string.  For a bit of performance,
 * we keep the list in least recently used order, which helps because
 * the calls to us come in pairs (one for malloc_enter and one right
 * after for malloc_leave).
 *=====================================================================
 */

const char *
debug_caller_loc(
    const char *file,
    int line)
{
    /*@keep@*/
    struct loc_str {
	char *str;
	LIST_ENTRY(loc_str) le;
    } *ls;
    static LIST_HEAD(, loc_str) root = LIST_HEAD_INITIALIZER(root);
    static char loc[256];	/* big enough for filename@lineno */
    const char *p;

    if ((p = strrchr(file, '/')) != NULL)
	file = p + 1;				/* just the last path element */

    snprintf(loc, SIZEOF(loc), "%s@%d", file, line);

    for (ls = LIST_FIRST(&root); ls != NULL; ls = LIST_NEXT(ls, le)) {
	if (strcmp(loc, ls->str) == 0) {
	    if (ls != LIST_FIRST(&root)) {
		/*
		 * This is a repeat and was not at the head of the list.
		 * Unlink it and move it to the front.
		 */
		LIST_REMOVE(ls, le);
		LIST_INSERT_HEAD(&root, ls, le);
	    }
	    return (ls->str);
	}
    }

    /*
     * This is a new entry.  Put it at the head of the list.
     */
    ls = malloc(SIZEOF(*ls));
    if (ls == NULL)
	return ("??");			/* not much better than abort */
    ls->str = malloc(strlen(loc) + 1);
    if (ls->str == NULL) {
	free(ls);
	return ("??");			/* not much better than abort */
    }
    strcpy(ls->str, loc);
    malloc_mark(ls);
    malloc_mark(ls->str);
    LIST_INSERT_HEAD(&root, ls, le);
    return (ls->str);
}

/*
 *=====================================================================
 * Save the current source line for vstralloc/newvstralloc.
 *
 * int debug_alloc_push (char *s, int l)
 *
 * entry:	s = source file
 *		l = source line
 * exit:	always zero
 * 
 * See the comments in amanda.h about what this is used for.
 *=====================================================================
 */

#define	DEBUG_ALLOC_SAVE_MAX	10

static struct {
	char		*file;
	int		line;
} debug_alloc_loc_info[DEBUG_ALLOC_SAVE_MAX];
static int debug_alloc_ptr = 0;

static char		*saved_file;
static int		saved_line;

int
debug_alloc_push(
    char *s,
    int l)
{
    debug_alloc_loc_info[debug_alloc_ptr].file = s;
    debug_alloc_loc_info[debug_alloc_ptr].line = l;
    debug_alloc_ptr = (debug_alloc_ptr + 1) % DEBUG_ALLOC_SAVE_MAX;
    return 0;
}

/*
 *=====================================================================
 * Pop the current source line information for vstralloc/newvstralloc.
 *
 * int debug_alloc_pop (void)
 *
 * entry:	none
 * exit:	none
 * 
 * See the comments in amanda.h about what this is used for.
 *=====================================================================
 */

void
debug_alloc_pop(void)
{
    debug_alloc_ptr =
      (debug_alloc_ptr + DEBUG_ALLOC_SAVE_MAX - 1) % DEBUG_ALLOC_SAVE_MAX;
    saved_file = debug_alloc_loc_info[debug_alloc_ptr].file;
    saved_line = debug_alloc_loc_info[debug_alloc_ptr].line;
}

/*
 * alloc - a wrapper for malloc.
 */
void *
debug_alloc(
    const char *s,
    int l,
    size_t size)
{
    void *addr;

    malloc_enter(debug_caller_loc(s, l));
    addr = (void *)malloc(max(size, 1));
    if (addr == NULL) {
	errordump("%s@%d: memory allocation failed (" SIZE_T_FMT " bytes requested)",
		  s ? s : "(unknown)",
		  s ? l : -1,
		  (SIZE_T_FMT_TYPE)size);
	/*NOTREACHED*/
    }
    malloc_leave(debug_caller_loc(s, l));
    return addr;
}


/*
 * newalloc - free existing buffer and then alloc a new one.
 */
void *
debug_newalloc(
    const char *s,
    int l,
    void *old,
    size_t size)
{
    char *addr;

    malloc_enter(debug_caller_loc(s, l));
    addr = debug_alloc(s, l, size);
    amfree(old);
    malloc_leave(debug_caller_loc(s, l));
    return addr;
}


/*
 * stralloc - copies the given string into newly allocated memory.
 *            Just like strdup()!
 */
char *
debug_stralloc(
    const char *s,
    int l,
    const char *str)
{
    char *addr;

    malloc_enter(debug_caller_loc(s, l));
    addr = debug_alloc(s, l, strlen(str) + 1);
    strcpy(addr, str);
    malloc_leave(debug_caller_loc(s, l));
    return (addr);
}

/* vstrextend -- Extends the existing string by appending the other 
 * arguments. */
/*@ignore@*/
arglist_function(
    char *vstrextend,
    char **, oldstr)
{
	char *keep = *oldstr;
	va_list ap;

	arglist_start(ap, oldstr);

	if (*oldstr == NULL)
		*oldstr = "";
	*oldstr = internal_vstralloc(*oldstr, ap);
        amfree(keep);

	arglist_end(ap);
        return *oldstr;
}
/*@end@*/

/*
 * internal_vstralloc - copies up to MAX_STR_ARGS strings into newly
 * allocated memory.
 *
 * The MAX_STR_ARGS limit is purely an efficiency issue so we do not have
 * to scan the strings more than necessary.
 */

#define	MAX_VSTRALLOC_ARGS	32

static char *
internal_vstralloc(
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
	errordump("internal_vstralloc: str is NULL");
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
	    errordump("%s@%d: more than %d args to vstralloc",
		      saved_file ? saved_file : "(unknown)",
		      saved_file ? saved_line : -1,
		      MAX_VSTRALLOC_ARGS);
	    /*NOTREACHED*/
	}
	arg[a] = next;
	len[a] = l;
	total_len += l;
	a++;
    }

    result = debug_alloc(saved_file, saved_line, total_len+1);

    next = result;
    for (b = 0; b < a; b++) {
	memcpy(next, arg[b], len[b]);
	next += len[b];
    }
    *next = '\0';

    return result;
}


/*
 * vstralloc - copies multiple strings into newly allocated memory.
 */
arglist_function(
    char *debug_vstralloc,
    const char *, str)
{
    va_list argp;
    char *result;

    debug_alloc_pop();
    malloc_enter(debug_caller_loc(saved_file, saved_line));
    arglist_start(argp, str);
    result = internal_vstralloc(str, argp);
    arglist_end(argp);
    malloc_leave(debug_caller_loc(saved_file, saved_line));
    return result;
}


/*
 * newstralloc - free existing string and then stralloc a new one.
 */
char *
debug_newstralloc(
    const char *s,
    int l,
    char *oldstr,
    const char *newstr)
{
    char *addr;

    malloc_enter(debug_caller_loc(s, l));
    addr = debug_stralloc(s, l, newstr);
    amfree(oldstr);
    malloc_leave(debug_caller_loc(s, l));
    return (addr);
}


/*
 * newvstralloc - free existing string and then vstralloc a new one.
 */
arglist_function1(
    char *debug_newvstralloc,
    char *, oldstr,
    const char *, newstr)
{
    va_list argp;
    char *result;

    debug_alloc_pop();
    malloc_enter(debug_caller_loc(saved_file, saved_line));
    arglist_start(argp, newstr);
    result = internal_vstralloc(newstr, argp);
    arglist_end(argp);
    amfree(oldstr);
    malloc_leave(debug_caller_loc(saved_file, saved_line));
    return result;
}


/*
 * safe_env - build a "safe" environment list.
 */
char **
safe_env(void)
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
#define SAFE_ENV_CNT	(size_t)(sizeof(safe_env_list) / sizeof(*safe_env_list))
    char **envp = safe_env_list + SAFE_ENV_CNT - 1;

    char **p;
    char **q;
    char *s;
    char *v;
    size_t l1, l2;

    if ((q = (char **)malloc(SIZEOF(safe_env_list))) != NULL) {
	envp = q;
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

/*
 * amtable_alloc -- (re)allocate enough space for some number of elements.
 *
 * input:	table -- pointer to pointer to table
 *		current -- pointer to current number of elements
 *		elsize -- size of a table element
 *		count -- desired number of elements
 *		bump -- round up factor
 *		init_func -- optional element initialization function
 * output:	table -- possibly adjusted to point to new table area
 *		current -- possibly adjusted to new number of elements
 */

int
debug_amtable_alloc(
    const char *s,
    int l,
    void **table,
    size_t *current,
    size_t elsize,
    size_t count,
    int bump,
    void (*init_func)(void *))
{
    void *table_new;
    size_t table_count_new;
    size_t i;

    if (count >= *current) {
	table_count_new = ((count + bump) / bump) * bump;
	table_new = debug_alloc(s, l, table_count_new * elsize);
	if (0 != *table) {
	    memcpy(table_new, *table, *current * elsize);
	    free(*table);
	}
	*table = table_new;
	memset(((char *)*table) + *current * elsize,
	       0,
	       (table_count_new - *current) * elsize);
	if (init_func != NULL) {
	    for (i = *current; i < table_count_new; i++) {
		(*init_func)(((char *)*table) + i * elsize);
	    }
	}
	*current = table_count_new;
    }
    return 0;
}

/*
 * amtable_free -- release a table.
 *
 * input:	table -- pointer to pointer to table
 *		current -- pointer to current number of elements
 * output:	table -- possibly adjusted to point to new table area
 *		current -- possibly adjusted to new number of elements
 */

void
amtable_free(
    void **table,
    size_t *current)
{
    amfree(*table);
    *current = 0;
}
