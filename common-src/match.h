/*
 * Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
 */

#ifndef MATCH_H
#define MATCH_H

#include <glib.h>
#include <conffile.h>

/*
 * Regular expressions
 */

/* The regular expressions used here are POSIX extended regular expressions;
 * see http://www.opengroup.org/onlinepubs/009695399/basedefs/xbd_chap09.html
 */

/* validate that REGEX is a valid POSIX regular expression by calling regcomp.
 * Returns a statically allocated error message on failure or NULL on success. */
char *	validate_regexp(const char *regex);

/*
 * Match the string "str" against POSIX regex "regex" with regexec(), with
 * REG_NEWLINE set (match_newline == TRUE) or not.
 *
 * REG_NEWLINE means two things:
 * - the dot won't match a newline;
 * - ^ and $ will match around \n in the input string (as well as the beginning
 *   and end of the input).
 */

int do_match(const char *regex, const char *str, gboolean match_newline);

#define match(regex, str) do_match(regex, str, TRUE)
#define match_no_newline(regex, str) do_match(regex, str, FALSE)

/*
 * Cleanup a regular expression by escaping all non alphanumeric characters, and
 * append beginning/end anchors if need be. Returns a dynamically allocated
 * string. It is the caller's responsibility to g_free() that string.
 *
 * @param str: the regular expression string
 * @param anchor: if true, the regex will be anchored at the beginning/end
 * (resp. with ^ and $)
 * @returns: the resulting regex
 */

char *clean_regex(const char *str, gboolean anchor);

/*
 * Globs
 */

/*
 * A "glob expression" is similar to shell globs; it supports metacharacters
 * "*" and "?", as well as character classes like "[...]" and "[!...]"
 * (negated).  The "*" and "?" do not match filename separators ("/").  The
 * entire expression is anchored, so it must match the string, not just a single
 * filename component.
 */

/* Validate that GLOB is a legal GLOB expression.  Returns a statically
 * allocated error message on failure, or NULL on success. */
char *	validate_glob(const char *glob);

/* Convert a GLOB expression into a dynamically allocated regular expression */
char *	glob_to_regex(const char *glob);

/* Like match(), but with a glob expression */
int	match_glob(const char *glob, const char *str);

/*
 * Tar Patterns
 */

/* A "tar expression" is almost the same as a glob, except that "*" can match a
 * filename separator ("?" cannot).  It is used by calcsize to emulate tar's exclude
 * list patterns, which are actually significantly more complicated than this.
 */

/* Like match(), but with a tar expression */
int	match_tar(const char *glob, const char *str);

/*
 * Host expressions
 */

/* Host expressions are described in amanda(8). */

/* Make an Amanda host expression that will match the given string exactly.
 * There is a little bit of fuzz here involving leading and trailing "."
 * chararacters, (so "host.org", "host.org.", and ".host.org" will all match
 * the same expressions) but DNS considers them equivalent, too. */
char *	make_exact_host_expression(const char *host);

/* Like match(), but using a host expression */
int	match_host(const char *glob, const char *host);

/*
 * Disk expressions
 */

/* Disk expressions are described in amanda(8) */

/* Make an Amanda disk expression that will match the given string exactly. */
char *	make_exact_disk_expression(const char *disk);

/* Like match(), but using a disk expression */
int	match_disk(const char *glob, const char *disk);

/*
 * Datestamp expressions
 */

/* Datestamp expressions are described in amanda(8) */

int	match_datestamp(const char *dateexp, const char *datestamp);

/*
 * Level expressions
 */

/* Level expressions are either prefix matches e.g., "1", which matches "1", "10", and "123",
 * absolute matches e.g., "3$" which only matches "3", or a range e.g., "3-5" which only
 * matches levels 3, 4, and 5. */

/* Like match(), but using a level expression */
int	match_level(const char *levelexp, const char *level);

/*
 * labelstr expressions
 */

int	match_labelstr_template(const char *template,
		       const char *label, const char *barcode, const char *meta);
int	match_labelstr(const labelstr_t *labelstr, const autolabel_t *autolabel,
		       const char *label, const char *barcode, const char *meta);

#endif /* MATCH_H */

