/*
 * Copyright (c) 2005 Zmanda Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
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
 * Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */
/*
 * Utility routines for handling sockaddrs
 */

#include "sockaddr-util.h"

void
dump_sockaddr(
    struct sockaddr_storage *sa)
{
#ifdef WORKING_IPV6
    char ipstr[INET6_ADDRSTRLEN];
#else
    char ipstr[INET_ADDRSTRLEN];
#endif
    int port;

    port = SS_GET_PORT(sa);
#ifdef WORKING_IPV6
    if (sa->ss_family == (sa_family_t)AF_INET6) {
	inet_ntop(AF_INET6, &((struct sockaddr_in6 *)sa)->sin6_addr,
		  ipstr, sizeof(ipstr));
	dbprintf("(sockaddr_in6 *)%p = { %d, %d, %s }\n",
		 sa,
		 ((struct sockaddr_in6 *)sa)->sin6_family,
		 port,
		 ipstr);
    } else
#endif
    {
	inet_ntop(AF_INET, &((struct sockaddr_in *)sa)->sin_addr, ipstr,
		  sizeof(ipstr));
	dbprintf("(sockaddr_in *)%p = { %d, %d, %s }\n",
		 sa,
		 ((struct sockaddr_in *)sa)->sin_family,
		 port,
		 ipstr);
    }
}


#ifdef WORKING_IPV6
static char mystr_sockaddr[INET6_ADDRSTRLEN + 20];
#else
static char mystr_sockaddr[INET_ADDRSTRLEN + 20];
#endif

char *
str_sockaddr(
    struct sockaddr_storage *sa)
{
#ifdef WORKING_IPV6
    char ipstr[INET6_ADDRSTRLEN];
#else
    char ipstr[INET_ADDRSTRLEN];
#endif
    int port;

    port = SS_GET_PORT(sa);
#ifdef WORKING_IPV6
    if ( sa->ss_family == (sa_family_t)AF_INET6) {
	inet_ntop(AF_INET6, &((struct sockaddr_in6 *)sa)->sin6_addr,
		  ipstr, sizeof(ipstr));
    } else
#endif
    {
	inet_ntop(AF_INET, &((struct sockaddr_in *)sa)->sin_addr, ipstr,
		  sizeof(ipstr));
    }
    snprintf(mystr_sockaddr,sizeof(mystr_sockaddr),"%s.%d", ipstr, port);
    return mystr_sockaddr;
}

int
cmp_sockaddr(
    struct sockaddr_storage *ss1,
    struct sockaddr_storage *ss2,
    int addr_only)
{
    if (ss1->ss_family == ss2->ss_family) {
        if (addr_only) {
#ifdef WORKING_IPV6
            if(ss1->ss_family == (sa_family_t)AF_INET6)
                return memcmp(
                    &((struct sockaddr_in6 *)ss1)->sin6_addr,
                    &((struct sockaddr_in6 *)ss2)->sin6_addr,
                    sizeof(((struct sockaddr_in6 *)ss1)->sin6_addr));
            else
#endif
                return memcmp(
                    &((struct sockaddr_in *)ss1)->sin_addr,
                    &((struct sockaddr_in *)ss2)->sin_addr,
                    sizeof(((struct sockaddr_in *)ss1)->sin_addr));
        } else {
            return memcmp(ss1, ss2, SS_LEN(ss1));
        }
    } else {
        /* compare families to give a total order */
        if (ss1->ss_family < ss2->ss_family)
            return -1;
        else
            return 1;
    }
}

