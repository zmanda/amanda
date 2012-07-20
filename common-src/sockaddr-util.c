/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */
/*
 * Utility routines for handling sockaddrs
 */

#include "amanda.h"
#include "sockaddr-util.h"

void
dump_sockaddr(
    sockaddr_union *sa)
{
#ifdef WORKING_IPV6
    char ipstr[INET6_ADDRSTRLEN];
#else
    char ipstr[INET_ADDRSTRLEN];
#endif
    int port;

    port = SU_GET_PORT(sa);
#ifdef WORKING_IPV6
    if (SU_GET_FAMILY(sa) == AF_INET6) {
	inet_ntop(AF_INET6, &sa->sin6.sin6_addr, ipstr, sizeof(ipstr));
	dbprintf("(sockaddr_in6 *)%p = { %d, %d, %s }\n",
		 sa,
		 SU_GET_FAMILY(sa),
		 port,
		 ipstr);
    } else
#endif
    {
	inet_ntop(AF_INET, &sa->sin.sin_addr.s_addr, ipstr, sizeof(ipstr));
	dbprintf("(sockaddr_in *)%p = { %d, %d, %s }\n",
		 sa,
		 SU_GET_FAMILY(sa),
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
    sockaddr_union *sa)
{
#ifdef WORKING_IPV6
    char ipstr[INET6_ADDRSTRLEN];
#else
    char ipstr[INET_ADDRSTRLEN];
#endif
    int port;

    port = SU_GET_PORT(sa);
#ifdef WORKING_IPV6
    if ( SU_GET_FAMILY(sa) == AF_INET6) {
	inet_ntop(AF_INET6, &sa->sin6.sin6_addr, ipstr, sizeof(ipstr));
    } else
#endif
    {
	inet_ntop(AF_INET, &sa->sin.sin_addr.s_addr, ipstr, sizeof(ipstr));
    }
    g_snprintf(mystr_sockaddr,sizeof(mystr_sockaddr),"%s:%d", ipstr, port);
    mystr_sockaddr[sizeof(mystr_sockaddr)-1] = '\0';

    return mystr_sockaddr;
}

char *
str_sockaddr_no_port(
    sockaddr_union *sa)
{
#ifdef WORKING_IPV6
    char ipstr[INET6_ADDRSTRLEN];
#else
    char ipstr[INET_ADDRSTRLEN];
#endif

#ifdef WORKING_IPV6
    if ( SU_GET_FAMILY(sa) == AF_INET6) {
	inet_ntop(AF_INET6, &sa->sin6.sin6_addr, ipstr, sizeof(ipstr));
    } else
#endif
    {
	inet_ntop(AF_INET, &sa->sin.sin_addr.s_addr, ipstr, sizeof(ipstr));
    }
    g_snprintf(mystr_sockaddr,sizeof(mystr_sockaddr),"%s", ipstr);
    mystr_sockaddr[sizeof(mystr_sockaddr)-1] = '\0';

    return mystr_sockaddr;
}

int
str_to_sockaddr(
	const char *src,
	sockaddr_union *dst)
{
    int result;

    g_debug("parsing %s", src);
    /* try AF_INET first */
    SU_INIT(dst, AF_INET);
    if ((result = inet_pton(AF_INET, src, &dst->sin.sin_addr)) == 1)
	return result;

    /* otherwise try AF_INET6, if supported */
#ifdef WORKING_IPV6
    SU_INIT(dst, AF_INET6);
    return inet_pton(AF_INET6, src, &dst->sin6.sin6_addr);
#else
    return result;
#endif
}

/* Unmap a V4MAPPED IPv6 address into its equivalent IPv4 address.  The location
 * TMP is used to store the rewritten address, if necessary.  Returns a pointer
 * to the unmapped address.
 */
#if defined(WORKING_IPV6) && defined(IN6_IS_ADDR_V4MAPPED)
static sockaddr_union *
unmap_v4mapped(
    sockaddr_union *sa,
    sockaddr_union *tmp)
{
    if (SU_GET_FAMILY(sa) == AF_INET6 && IN6_IS_ADDR_V4MAPPED(&sa->sin6.sin6_addr)) {
	SU_INIT(tmp, AF_INET);
	SU_SET_PORT(tmp, SU_GET_PORT(sa));
	/* extract the v4 address from byte 12 of the v6 address */
	memcpy(&tmp->sin.sin_addr.s_addr,
	       &sa->sin6.sin6_addr.s6_addr[12],
	       sizeof(struct in_addr));
	return tmp;
    }

    return sa;
}
#else
/* nothing to do if no IPv6 */
#define unmap_v4mapped(sa, tmp) ((void)tmp, sa)
#endif

int
cmp_sockaddr(
    sockaddr_union *ss1,
    sockaddr_union *ss2,
    int addr_only)
{
    sockaddr_union tmp1, tmp2;

    /* if addresses are v4mapped, "unmap" them */
    ss1 = unmap_v4mapped(ss1, &tmp1);
    ss2 = unmap_v4mapped(ss2, &tmp2);

    if (SU_GET_FAMILY(ss1) == SU_GET_FAMILY(ss2)) {
        if (addr_only) {
#ifdef WORKING_IPV6
            if(SU_GET_FAMILY(ss1) == AF_INET6)
                return memcmp(
                    &ss1->sin6.sin6_addr,
                    &ss2->sin6.sin6_addr,
                    sizeof(ss1->sin6.sin6_addr));
            else
#endif
                return memcmp(
                    &ss1->sin.sin_addr,
                    &ss2->sin.sin_addr,
                    sizeof(ss1->sin.sin_addr));
        } else {
            return memcmp(ss1, ss2, SS_LEN(ss1));
        }
    } else {
        /* compare families to give a total order */
        if (SU_GET_FAMILY(ss1) < SU_GET_FAMILY(ss2))
            return -1;
        else
            return 1;
    }
}

