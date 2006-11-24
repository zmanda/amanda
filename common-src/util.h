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

#define BIGINT  INT_MAX

#define BSTRNCMP(a,b)  strncmp(a, b, strlen(b))

/* internal types and variables */


ssize_t	fullread(int, void *, size_t);
ssize_t	fullwrite(int, const void *, size_t);

int	connect_portrange(struct sockaddr_storage *, in_port_t, in_port_t, char *,
			  struct sockaddr_storage *, int);
int	bind_portrange(int, struct sockaddr_storage *, in_port_t, in_port_t,
		       char *);

char *	construct_datestamp(time_t *t);
char *	construct_timestamp(time_t *t);

/*@only@*//*@null@*/char *quote_string(const char *str);
/*@only@*//*@null@*/char *unquote_string(const char *str);
int	needs_quotes(const char * str);

char *	sanitize_string(const char *str);
char *	strquotedstr(void);
ssize_t	hexdump(const char *buffer, size_t bytes);
void	dump_sockaddr(struct sockaddr_storage *	sa);
char *  str_sockaddr(struct sockaddr_storage *sa);
int     cmp_sockaddr(struct sockaddr_storage *ss1,
		     struct sockaddr_storage *ss2);
int     copy_file(char *dst, char *src, char **errmsg);

/*
 *   validate_email return 0 if the following characters are present
 *   * ( ) < > [ ] , ; : ! $ \ / "
 *   else returns 1
 */
int validate_mailto(const char *mailto);

char *taperalgo2str(int taperalgo);

#endif	/* UTIL_H */
