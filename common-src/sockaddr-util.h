/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */
/*
 * Utility routines for handling sockaddrs
 */

#ifndef SOCKADDR_UTIL_H
#define	SOCKADDR_UTIL_H

#include <arpa/inet.h>

/* Unfortunately, the system-level sockaddr_storage definition can lead to
 * C aliasing errors (where the optimizer doesn't notice that two operations
 * affect the same datum).  We define our own similar type as a union.
 */
typedef union sockaddr_union {
    struct sockaddr         sa;
    struct sockaddr_in      sin;
#ifdef WORKING_IPV6
    struct sockaddr_in6     sin6;
#endif
#ifdef HAVE_SOCKADDR_STORAGE
    struct sockaddr_storage ss; /* not used; just here to make the union full-size */
#endif
} sockaddr_union;

typedef struct amsbind_s {
    sockaddr_union addr;
    socklen_t_equiv socklen;
} ambind_t;

/* Dump a sockaddr_union using dbprintf
 *
 * @param sa: the sockaddr to dump
 */
void	dump_sockaddr(sockaddr_union *	sa);

/* Convert a sockaddr_union to a string.
 *
 * NOTE: this function is not threadsafe!
 *
 * @param sa: the sockaddr_union to dump
 * @returns: pointer to statically allocated string
 */
char *  str_sockaddr(sockaddr_union *sa);
char *  str_sockaddr_no_port(sockaddr_union *sa);

/* same but threadsafe, the string is put in strsockaddr
 */
char *  str_sockaddr_r(sockaddr_union *sa, char *strsockaddr, socklen_t size);
char *  str_sockaddr_no_port_r(sockaddr_union *sa, char *strsockaddr,
			       socklen_t size);

/* Compare two sockaddr_union objects, optionally comparing
 * only the address (and thus ignoring port, flow info, etc.).
 *
 * @param su1: one sockaddr_union to compare
 * @param su2: the other sockaddr_union to compare
 * @param addr_only: if true, ignore port, flow info, etc.
 * @returns: -1, 0, or 1 for <, ==, >, respectively
 */
int     cmp_sockaddr(sockaddr_union *su1,
		     sockaddr_union *su2,
		     int addr_only);

/* Parse a string into a sockaddr.  This will try all available address
 * families.
 *
 * @param src: the string representation of the address
 * @param dst: the sockaddr_union in which to store the result
 * @returns: 1 on success, -1 on error, or 0 if unparseable
 */
int	str_to_sockaddr(
	const char *src,
	sockaddr_union *dst);

/* Copy a sockaddr object.
 *
 * @param dest: destination
 * @param src: source
 */
#define copy_sockaddr(dest, src) memcpy((dest), (src), SS_LEN((src)))

/* The "best" address family we support.
 */
/* AF_NATIVE */
#ifdef WORKING_IPV6
#define AF_NATIVE AF_INET6
#else
#define AF_NATIVE AF_INET
#endif

/* Get the family for a sockaddr_union.
 *
 * @param su: the sockaddr_union to examine
 */
#define SU_GET_FAMILY(su) ((su)->sa.sa_family)
/* Calculate the length of the data in a sockaddr_union.
 *
 * @param su: the sockaddr_union to examine
 * @returns: length of the data in the object
 */
/* SS_LEN(su) */
#ifdef WORKING_IPV6
# define SS_LEN(su) (SU_GET_FAMILY(su)==AF_INET6? sizeof(struct sockaddr_in6):sizeof(struct sockaddr_in))
#else
# define SS_LEN(su) (sizeof(struct sockaddr_in))
#endif

/* Initialize a sockaddr_union to all zeroes (as directed by RFC),
 * and set its address family as specified
 *
 * @param su: sockaddr_union object to initialize
 * @param family: an AF_* constant
 */
/* SU_INIT(su, family) */
#define SU_INIT(su, family) do { \
    memset((su), 0, sizeof(*(su))); \
    (su)->sa.sa_family = (family); \
} while (0);

/* set a sockaddr_union to the family-appropriate equivalent of
 * INADDR_ANY -- a wildcard address and port.  Call SU_INIT(su)
 * first to initialize the object and set the family.
 *
 * @param su: the sockaddr_union to set
 */
/* SU_SET_INADDR_ANY(su) */
#ifdef WORKING_IPV6
#define SU_SET_INADDR_ANY(su) do { \
    switch (SU_GET_FAMILY(su)) { \
        case AF_INET6: \
            (su)->sin6.sin6_flowinfo = 0; \
            (su)->sin6.sin6_addr = in6addr_any; \
            break; \
        case AF_INET: \
            (su)->sin.sin_addr.s_addr = INADDR_ANY; \
            break; \
    } \
} while (0);
#else
#define SU_SET_INADDR_ANY(su) do { \
    (su)->sin.sin_addr.s_addr = INADDR_ANY; \
} while (0);
#endif

/* Set the IP in a sockaddr_union that already has an family
 *
 * @param su: the sockaddr_union to manipulate
 * @param ip: the IP to insert
 */
/* SU_SET_INADDR(su, ip) */
#ifdef WORKING_IPV6
#define SU_SET_INADDR(su, ip) do { \
    switch (SU_GET_FAMILY(su)) { \
	case AF_INET6: \
            (su)->sin6.sin6_flowinfo = 0; \
	    inet_pton(SU_GET_FAMILY(su), ip, &((su)->sin6.sin6_addr)); \
	    break; \
	case AF_INET: \
	    inet_pton(SU_GET_FAMILY(su), ip, &((su)->sin.sin_addr)); \
	    break; \
    } \
} while (0);
#else
#define SU_SET_INADDR(su, ip) \
    inet_pton(SU_GET_FAMILY(su), ip, &((su)->sin.sin_addr));
#endif

/* Set the port in a sockaddr_union that already has an family
 *
 * @param su: the sockaddr_union to manipulate
 * @param port: the port to insert (in host byte order)
 */
/* SU_SET_PORT(su, port) */
#ifdef WORKING_IPV6
#define SU_SET_PORT(su, port) \
switch (SU_GET_FAMILY(su)) { \
    case AF_INET: \
        (su)->sin.sin_port = (in_port_t)htons((port)); \
        break; \
    case AF_INET6: \
        (su)->sin6.sin6_port = (in_port_t)htons((port)); \
        break; \
    default: assert(0); \
}
#else
#define SU_SET_PORT(su, port) \
        (su)->sin.sin_port = (in_port_t)htons((port));
#endif

/* Get the port in a sockaddr_union object
 *
 * @param su: the sockaddr_union to manipulate
 * @return: the port, in host byte horder
 */
/* SU_GET_PORT(su) */
#ifdef WORKING_IPV6
#define SU_GET_PORT(su) (ntohs(SU_GET_FAMILY(su) == AF_INET6? (su)->sin6.sin6_port:(su)->sin.sin_port))
#else
#define SU_GET_PORT(su) (ntohs((su)->sin.sin_port))
#endif

#endif	/* SOCKADDR_UTIL_H */
