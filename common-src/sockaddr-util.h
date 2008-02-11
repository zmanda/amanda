/*
 * Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */
/*
 * Utility routines for handling sockaddrs
 */

#ifndef SOCKADDR_H
#define	SOCKADDR_H

#include "amanda.h"

/* Dump a sockaddr_storage using dbprintf
 *
 * @param sa: the sockaddr to dump
 */
void	dump_sockaddr(struct sockaddr_storage *	sa);

/* Convert a sockaddr_storage to a string.
 *
 * NOTE: this function is not threadsafe!
 *
 * @param sa: the sockaddr_storage to dump
 * @returns: pointer to statically allocated string
 */
char *  str_sockaddr(struct sockaddr_storage *sa);

/* Compare two sockaddr_storage objects, optionally comparing
 * only the address (and thus ignoring port, flow info, etc.).
 *
 * @param ss1: one sockaddr_storage to compare
 * @param ss2: the other sockaddr_storage to compare
 * @param addr_only: if true, ignore port, flow info, etc.
 * @returns: -1, 0, or 1 for <, ==, >, respectively
 */
int     cmp_sockaddr(struct sockaddr_storage *ss1,
		     struct sockaddr_storage *ss2,
		     int addr_only);

/* Copy a sockaddr object.
 *
 * @param dest: destination
 * @param src: source
 */
#define copy_sockaddr(dest, src) memcpy((dest), (src), SS_LEN((src)))

/* Calculate the length of the data in a struct sockaddr_storage.
 *
 * @param ss: the sockaddr_storage to examine
 * @returns: length of the data in the object
 */
/* SS_LEN(ss) */
#ifdef WORKING_IPV6
# define SS_LEN(ss) (((struct sockaddr *)(ss))->sa_family==AF_INET6?sizeof(struct sockaddr_in6):sizeof(struct sockaddr_in))
#else
# define SS_LEN(ss) (sizeof(struct sockaddr_in))
#endif

/* The "best" address family we support.
 */
/* AF_NATIVE */
#ifdef WORKING_IPV6
#define AF_NATIVE AF_INET6
#else
#define AF_NATIVE AF_INET
#endif

/* Initialize a sockaddr_storage to all zeroes (as directed by RFC),
 * and set its ss_family as specified
 *
 * @param ss: sockaddr_storage object to initialize
 * @param family: an AF_* constant
 */
/* SS_INIT(ss, family) */
#define SS_INIT(ss, family) do { \
    memset((ss), 0, sizeof(*(ss))); \
    (ss)->ss_family = (family); \
} while (0);

/* set a sockaddr_storage to the family-appropriate equivalent of
 * INADDR_ANY -- a wildcard address and port.  Call SS_INIT(ss)
 * first to initialize the object and set the family.
 *
 * @param ss: the sockaddr_storage to set
 */
/* SS_SET_INADDR_ANY(ss) */
#ifdef WORKING_IPV6
#define SS_SET_INADDR_ANY(ss) do { \
    switch ((ss)->ss_family) { \
        case AF_INET6: \
            ((struct sockaddr_in6 *)(ss))->sin6_flowinfo = 0; \
            ((struct sockaddr_in6 *)(ss))->sin6_addr = in6addr_any; \
            break; \
        case AF_INET: \
            ((struct sockaddr_in *)(ss))->sin_addr.s_addr = INADDR_ANY; \
            break; \
    } \
} while (0);
#else
#define SS_SET_INADDR_ANY(ss) do { \
    ((struct sockaddr_in *)(ss))->sin_addr.s_addr = INADDR_ANY; \
} while (0);
#endif

/* Set the port in a sockaddr_storage that already has an family
 *
 * @param ss: the sockaddr_storage to manipulate
 * @param port: the port to insert
 */
/* SS_SET_PORT(ss, port) */
#ifdef WORKING_IPV6
#define SS_SET_PORT(ss, port) \
switch ((ss)->ss_family) { \
    case AF_INET: \
        ((struct sockaddr_in *)(ss))->sin_port = (in_port_t)htons((port)); \
        break; \
    case AF_INET6: \
        ((struct sockaddr_in6 *)(ss))->sin6_port = (in_port_t)htons((port)); \
        break; \
    default: assert(0); \
}
#else
#define SS_SET_PORT(ss, port) \
        ((struct sockaddr_in *)(ss))->sin_port = (in_port_t)htons((port));
#endif

/* Get the port in a sockaddr_storage object
 *
 * @param ss: the sockaddr_storage to manipulate
 */
/* SS_GET_PORT(ss) */
#ifdef WORKING_IPV6
#define SS_GET_PORT(ss) (ntohs( \
       (ss)->ss_family == AF_INET6? \
        ((struct sockaddr_in6 *)(ss))->sin6_port \
       :((struct sockaddr_in *)(ss))->sin_port))
#else
#define SS_GET_PORT(ss) (ntohs( \
        ((struct sockaddr_in *)(ss))->sin_port))
#endif

#endif	/* SOCKADDR_H */

