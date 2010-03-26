/*
 * Copyright (c) 2010 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#ifndef MATCH_H
#define MATCH_H

#include <glib.h>

char *	validate_regexp(const char *regex);

/* quote any non-alphanumeric characters in str, so that the result will only
 * match the original string.  If anchor is true, then add ^ and $ to make sure
 * that substrings will not match.  Note that the resulting regular expression
 * will not work with match_host, match_disk, etc., since those do not technically
 * support quoting metacharacters */
char *	clean_regex(const char *str, gboolean anchor);

/* Make an Amanda host expression that will match the given string exactly. */
char *	make_exact_host_expression(const char *host);

/* Make an Amanda disk expression that will match the given string exactly. */
char *	make_exact_disk_expression(const char *disk);

int	match(const char *regex, const char *str);

int	match_no_newline(const char *regex, const char *str);

char *	validate_glob(const char *glob);

char *	glob_to_regex(const char *glob);

int	match_glob(const char *glob, const char *str);

int	match_tar(const char *glob, const char *str);

int	match_host(const char *glob, const char *host);

int	match_datestamp(const char *dateexp, const char *datestamp);

int	match_level(const char *levelexp, const char *level);

int	match_disk(const char *glob, const char *disk);

#endif /* MATCH_H */

