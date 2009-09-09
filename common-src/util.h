/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1999 University of Maryland at College Park
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
 * $Id: util.h,v 1.17 2006/07/26 15:17:36 martinea Exp $
 */
#ifndef UTIL_H
#define	UTIL_H

#include "amanda.h"
#include "sl.h"

#include <glib.h>
#include <glib-object.h>
#include <regex.h>

#include "glib-util.h"

#define BIGINT  INT_MAX

#define BSTRNCMP(a,b)  strncmp(a, b, strlen(b))

/* internal types and variables */


int	connect_portrange(sockaddr_union *, in_port_t, in_port_t, char *,
			  sockaddr_union *, int);
int	bind_portrange(int, sockaddr_union *, in_port_t, in_port_t,
		       char *);

ssize_t	full_writev(int, struct iovec *, int);

char *	construct_datestamp(time_t *t);
char *	construct_timestamp(time_t *t);

/* quote_string only adds "" if they're required; quote_string_always
 * always adds "" around the string */
#define quote_string(str) quote_string_maybe((str), 0)
#define quote_string_always(str) quote_string_maybe((str), 1)

/*@only@*//*@null@*/char *quote_string_maybe(const char *str, gboolean always);
/*@only@*//*@null@*/char *unquote_string(const char *str);

/* Split a string into space-delimited words, obeying quoting as created by
 * quote_string.  To keep compatibility with the old split(), this has the
 * characteristic that multiple consecutive spaces are not collapsed into
 * a single space: "x  y" parses as [ "x", "", "y", NULL ].  The strings are
 * unquoted before they are returned, unlike split().  An empty string is
 * split into [ "", NULL ].
 *
 * Returns a NULL-terminated array of strings, which should be freed with
 * g_strfreev.
 */
gchar ** split_quoted_strings(const gchar *string);

/* Like strtok_r, but consider a quoted string to be a single token.  Caller
 * must begin parsing with strtok_r first, then pass the saveptr to this function.
 *
 * Returns NULL on unparseable strings (e.g., unterminated quotes, bad escapes)
 */
char *		strquotedstr(char **saveptr);

char *	sanitize_string(const char *str);
int     copy_file(char *dst, char *src, char **errmsg);

/* These two functions handle "braced alternates", which is a syntax borrowed,
 * partially, from shells.  Comma-separated strings enclosed in curly braces
 * expand into multiple alternatives for the entire string.
 * For example:
 *
 *   "{foo,bar,bat}" -> [ "foo", "bar", "bat" ]
 *   "foo{1,2}bar" -> [ "foo1bar", "foo2bar" ]
 *   "foo{1\,2,3}bar" -> [ "foo1,2bar", "foo3bar" ]
 *   "{a,b}-{1,2}" -> [ "a-1", "a-2", "b-1", "b-2" ]
 *
 * Note that nested braces are not processed.  Braces, commas, and backslashes
 * may be escaped with backslashes.  Returns NULL on invalid strings.
 */
GPtrArray * expand_braced_alternates(char * source);
char * collapse_braced_alternates(GPtrArray *source);

/*
 *   validate_email return 0 if the following characters are present
 *   * ( ) < > [ ] , ; : ! $ \ / "
 *   else returns 1
 */
int validate_mailto(const char *mailto);

/* This function is a portable reimplementation of readdir(). It
 * returns a newly-allocated string, that should be freed with
 * free(). Returns NULL on error or end of directory.
 * It is reentrant, with the following exceptions:
 * - This function cannot be run at the same time as readdir() or
 *   readdir64().
 * - This function cannot be run simultaneously on the same directory
 *   handle. */
char * portable_readdir(DIR*);

typedef gboolean (*SearchDirectoryFunctor)(const char * filename,
                                           gpointer user_data);
/* This function will search the given directory handle for files
   matching the given POSIX extended regular expression.
   For each matching file, the functor will be called with the given
   user data. Stops when the functor returns FALSE, or all files have
   been searched. Returns the number of matching files. */
int search_directory(DIR * handle, const char * regex,
                     SearchDirectoryFunctor functor, gpointer user_data);

/* This function extracts a substring match from a regular expression
   match result, and copies it into a newly allocated string. Example
   usage to get the first matched substring:
   substring = find_regmatch(whole_string, pmatch[1])
   Note that pmatch[0] yields the entire matching portion of the string. */
char* find_regex_substring(const char* base_string, const regmatch_t match);

void free_new_argv(int new_argc, char **new_argv);

/* Like strcmp(a, b), except that NULL strings are sorted before non-NULL
 * strings, instead of segfaulting. */
int compare_possibly_null_strings(const char * a, const char * b);

/* Given a hostname, call getaddrinfo to resolve it.  Optionally get the
 * entire set of results (if res is not NULL) and the canonical name of
 * the host (if canonname is not NULL).  The canonical name might
 * expand e.g., www.domain.com to server3.webfarm.hosting.com.
 *
 * If not NULL, the caller is responsible for freeing res with freeaddrinfo().
 * Similarly, the caller is responsible for freeing canonname if it is
 * not NULL.
 *
 * @param hostname: the hostname to start with
 * @param res: (result) if not NULL, the results from getaddrinfo()
 * @param canonname: (result) if not NULL, the canonical name of the host
 * @returns: newly allocated canonical hostname, or NULL if no
 * canonical hostname was available.
 */
int resolve_hostname(const char *hostname, int socktype,
		     struct addrinfo **res, char **canonname);

/* Interpret a status (as returned from wait() and friends)
 * into a human-readable sentence.
 *
 * Caller is responsible for freeing the resulting string.
 * The resulting string has already been translated.
 *
 * The macro definition allows this to work even when amwait_t
 * is 'union wait' (4.3BSD).  The cast is safe because the two
 * argument types are interchangeable.
 *
 * @param subject: subject of the sentence (program name, etc.)
 * @param status: the exit status
 * @returns: newly allocated string describing status
 */
#define str_exit_status(subject, status) \
    _str_exit_status((subject), *(amwait_t *)&(status))
char *_str_exit_status(char *subject, amwait_t status);

/*
 * Userid manipulation
 */

/* Check that the current uid and euid are set to a specific user, 
 * calling error() if not. Does nothing if CHECK_USERID is not 
 * defined.  
 *
 * @param who: one of the RUNNING_AS_* constants, below.
 */
typedef enum {
        /* doesn't matter */
    RUNNING_AS_ANY,

        /* userid is 0 */
    RUNNING_AS_ROOT,

        /* userid belongs to dumpuser (from config) */
    RUNNING_AS_DUMPUSER,

        /* prefer that userid belongs to dumpuser, but accept when userid belongs to
         * CLIENT_LOGIN with a debug-log message (needed because amandad always runs
         * as CLIENT_LOGIN, even on server) */
    RUNNING_AS_DUMPUSER_PREFERRED,

        /* userid belongs to CLIENT_LOGIN (from --with-user) */
    RUNNING_AS_CLIENT_LOGIN,

    RUNNING_AS_USER_MASK = (1 << 8) - 1,
	/* '&' this on to only check the uid, not the euid; use this for programs
	 * that will call become_root() */
    RUNNING_AS_UID_ONLY = 1 << 8
} running_as_flags;

void check_running_as(running_as_flags who);

/* Drop and regain root priviledges; used from setuid-root binaries which only
 * need to be root for certain operations. Does nothing if SINGLE_USERID is 
 * defined.
 *
 * @param need_root: if 1, try to assume root priviledges; otherwise, drop
 * priviledges.  If -1, drop them irreversibly.
 * @returns: true if the priviledge change succeeded
 */
int set_root_privs(int need_root);

/* Become root completely, by setting the uid to 0.  This is used by setuid-root
 * apps which will exec subprocesses which will also need root priviledges.  Does
 * nothing if SINGLE_USERID is defined.
 *
 * @returns: true if the priviledge change succeeded
 */
int become_root(void);

/*
 * Process parameters
 */

/* The 'context' of a process gives a general description of how it is
 * used.  This affects log output, among other things.
 */
typedef enum {
    /* default context (logging to stderr, etc. -- not pretty) */
    CONTEXT_DEFAULT = 0,

    /* user-interfacing command-line utility like amadmin */
    CONTEXT_CMDLINE,

    /* daemon like amandad or sendbackup */
    CONTEXT_DAEMON,

    /* a utility used from shell scripts, and thus probably invoked
     * quite often */
    CONTEXT_SCRIPTUTIL,
} pcontext_t;

/* Set the name of the process.  The parameter is copied, and remains
 * the responsibility of the caller on return. This value is used in log
 * messages and other output throughout Amanda.
 *
 * @param pname: the new process name
 */
void set_pname(char *pname);

/* Get the current process name; the result is in a static buffer, and
 * should *not* be free()d by the caller.
 *
 * @returns: process name
 */
char *get_pname(void);

/* Set the type of the process.  The parameter is copied, and remains
 * the responsibility of the caller on return.  This value dictates the
 * directory in which debug logs are stored.
 *
 * @param pname: the new process type
 */
void set_ptype(char *ptype);

/* Get the current process name; the result is in a static buffer, and
 * should *not* be free()d by the caller.
 *
 * @returns: process name
 */
char *get_ptype(void);

/* Set the process's context
 *
 * @param context: the new context
 */
void set_pcontext(pcontext_t context);

/* Get the process's context
 *
 * @returns: the context
 */
pcontext_t get_pcontext(void);

/*
 * Readline support
 *
 * This either includes the system readline header we found in configure,
 * or prototypes some simple stub functions that are used instead.
 */

#ifdef HAVE_READLINE
#  ifdef HAVE_READLINE_READLINE_H
#    include <readline/readline.h>
#    ifdef HAVE_READLINE_HISTORY_H
#      include <readline/history.h>
#    endif
#  else
#    ifdef HAVE_READLINE_H
#      include <readline.h>
#      ifdef HAVE_HISTORY_H
#        include <history.h>
#      endif
#    endif
#  endif
#else

char *	readline(const char *prompt);
void	add_history(const char *line);

#endif

char *base64_decode_alloc_string(char *);

/* A GHFunc (callback for g_hash_table_foreach),
 * Count the number of properties.
 *
 * @param key_p: (char *) property name.
 * @param value_p: (GSList *) property values list.
 * @param user_data_p: (int *) count are added to that value.
 */
void count_proplist(gpointer key_p,
		    gpointer value_p,
		    gpointer user_data_p);

/* A GHFunc (callback for g_hash_table_foreach),
 * Store a property and it's value in an ARGV.
 *
 * @param key_p: (char *) property name.
 * @param value_p: (GSList *) property values list.
 * @param user_data_p: (char ***) pointer to ARGV.
 */
void proplist_add_to_argv(gpointer key_p,
			  gpointer value_p,
			  gpointer user_data_p);


#endif	/* UTIL_H */
